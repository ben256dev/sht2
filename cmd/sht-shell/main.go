package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

var (
	sockDir = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht               # upload stdin")
	fmt.Fprintln(os.Stderr, "  sht cat     [<digest> ...] # print blobs; reads whitespace-delimited digests from stdin when none are given")
	fmt.Fprintln(os.Stderr, "  sht stat    [<digest> ...] # stat blobs; reads whitespace-delimited digests from stdin when none are given")
	fmt.Fprintln(os.Stderr, "  sht release [<digest> ...] # release blobs; reads whitespace-delimited digests from stdin when none are given")
	fmt.Fprintln(os.Stderr, "  sht list [-dskcta] # list refs; d=digest, s=size, k=key_id, c=created_at, t=state, a=all")
	fmt.Fprintln(os.Stderr, "  sht manifest       # create/resume upload from JSON manifest on stdin")
	fmt.Fprintln(os.Stderr, "  sht upload-chunk <digest> <index> # upload raw chunk bytes from stdin")
	fmt.Fprintln(os.Stderr, "  sht upload-status <digest> # show resumable upload status")
	fmt.Fprintln(os.Stderr, "  sht finalize <digest> # finalize a complete resumable upload")
	fmt.Fprintln(os.Stderr, "  sht help          # show this message")
}

func catUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht cat [<digest> ...]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Print blobs for whitespace-delimited digests.")
	fmt.Fprintln(os.Stderr, "When no digests are given as arguments, digests are read from stdin.")
}

func statUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht stat [<digest> ...]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Check blobs for whitespace-delimited digests.")
	fmt.Fprintln(os.Stderr, "When no digests are given as arguments, digests are read from stdin.")
}

func releaseUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht release [<digest> ...]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Release refs for whitespace-delimited digests.")
	fmt.Fprintln(os.Stderr, "When no digests are given as arguments, digests are read from stdin.")
}

func listUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht list [-dskcta]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "fields:")
	fmt.Fprintln(os.Stderr, "  -d  digest")
	fmt.Fprintln(os.Stderr, "  -s  size")
	fmt.Fprintln(os.Stderr, "  -k  key_id")
	fmt.Fprintln(os.Stderr, "  -c  created_at")
	fmt.Fprintln(os.Stderr, "  -t  state (clean or dirty)")
	fmt.Fprintln(os.Stderr, "  -a  all fields (default)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "examples:")
	fmt.Fprintln(os.Stderr, "  sht list")
	fmt.Fprintln(os.Stderr, "  sht list -d")
	fmt.Fprintln(os.Stderr, "  sht list -dt")
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

type StoreBlobResponse struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Exists bool   `json:"exists"`
}

type BlobRef struct {
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	KeyID     int64  `json:"key_id"`
	CreatedAt string `json:"created_at"`
	Dirty     bool   `json:"dirty"`
}

type ListRefsResponse struct {
	Refs []BlobRef `json:"refs"`
}

type UploadStatusResponse struct {
	Digest          string  `json:"digest"`
	Size            int64   `json:"size"`
	ChunkSize       int64   `json:"chunk_size"`
	Missing         []int64 `json:"missing"`
	Uploaded        []int64 `json:"uploaded"`
	Complete        bool    `json:"complete"`
	Finalized       bool    `json:"finalized"`
	PhysicalPending int64   `json:"physical_pending_bytes,omitempty"`
}

type ChunkUploadResponse struct {
	Digest      string `json:"digest"`
	Index       int64  `json:"index"`
	ChunkDigest string `json:"chunk_digest"`
	Size        int64  `json:"size"`
	Exists      bool   `json:"exists"`
}

func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}
}

func collectDigests(args []string, command string) []string {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		switch command {
		case "cat":
			catUsage()
		case "stat":
			statUsage()
		case "release":
			releaseUsage()
		}
		os.Exit(0)
	}

	if len(args) > 0 {
		return args
	}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		die("failed to read digests from stdin: %v", err)
	}

	digests := strings.Fields(string(data))
	if len(digests) == 0 {
		die("usage: sht %s [<digest> ...]", command)
	}
	return digests
}

func store(keyID int64) {
	client := newClient()

	req, err := http.NewRequest(http.MethodPost, "http://sht/blob", os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(os.Stderr, resp.Body)
		os.Exit(1)
	}

	var out StoreBlobResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		die("bad response: %v", err)
	}

	fmt.Println(out.Digest)
}

func catOne(client *http.Client, digest string, keyID int64) bool {
	req, err := http.NewRequest(http.MethodGet, "http://sht/blob/"+digest, nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%s: ", digest)
		io.Copy(os.Stderr, resp.Body)
		return false
	}

	io.Copy(os.Stdout, resp.Body)
	return true
}

func cat(digests []string, keyID int64) {
	client := newClient()
	ok := true
	for _, digest := range digests {
		if !catOne(client, digest, keyID) {
			ok = false
		}
	}
	if !ok {
		os.Exit(1)
	}
}

func statOne(client *http.Client, digest string, keyID int64) (int, error) {
	req, err := http.NewRequest(http.MethodHead, "http://sht/blob/"+digest, nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func stat(digests []string, keyID int64) {
	client := newClient()
	multiple := len(digests) > 1
	ok := true

	for _, digest := range digests {
		code, err := statOne(client, digest, keyID)
		if err != nil {
			log.Fatal(err)
		}
		switch code {
		case http.StatusOK:
			if multiple {
				fmt.Println(digest, "exists")
			} else {
				fmt.Println("exists")
			}
		case http.StatusNotFound:
			if multiple {
				fmt.Fprintln(os.Stderr, digest, "not found")
			} else {
				fmt.Fprintln(os.Stderr, "not found")
			}
			ok = false
		default:
			if multiple {
				fmt.Fprintln(os.Stderr, digest, http.StatusText(code))
			} else {
				fmt.Fprintln(os.Stderr, code)
			}
			ok = false
		}
	}

	if !ok {
		os.Exit(1)
	}
}

func releaseOne(client *http.Client, digest string, keyID int64) (int, error) {
	req, err := http.NewRequest(http.MethodDelete, "http://sht/blob/"+digest, nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func release(digests []string, keyID int64) {
	client := newClient()
	multiple := len(digests) > 1
	ok := true

	for _, digest := range digests {
		code, err := releaseOne(client, digest, keyID)
		if err != nil {
			log.Fatal(err)
		}
		switch code {
		case http.StatusNoContent:
			if multiple {
				fmt.Println(digest, "released")
			} else {
				fmt.Println("released")
			}
		case http.StatusNotFound:
			if multiple {
				fmt.Fprintln(os.Stderr, digest, "not found")
			} else {
				fmt.Fprintln(os.Stderr, "not found")
			}
			ok = false
		default:
			if multiple {
				fmt.Fprintln(os.Stderr, digest, http.StatusText(code))
			} else {
				fmt.Fprintln(os.Stderr, code)
			}
			ok = false
		}
	}

	if !ok {
		os.Exit(1)
	}
}

func parseListFields(args []string) string {
	if len(args) == 0 {
		return "a"
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		listUsage()
		os.Exit(0)
	}
	if len(args) != 1 || !strings.HasPrefix(args[0], "-") || args[0] == "-" {
		die("usage: sht list [-dskcta]")
	}

	fields := strings.TrimPrefix(args[0], "-")
	for _, field := range fields {
		switch field {
		case 'd', 's', 'k', 'c', 't', 'a':
		default:
			die("unknown list field: %c", field)
		}
	}
	return fields
}

func refState(ref BlobRef) string {
	if ref.Dirty {
		return "dirty"
	}
	return "clean"
}

func stdoutIsTerminal() bool {
	info, err := os.Stdout.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func printRefLine(ref BlobRef, line string, dimDirty bool) {
	if dimDirty && ref.Dirty {
		fmt.Printf("\033[2m%s\033[0m\n", line)
		return
	}
	fmt.Println(line)
}

func refs(keyID int64, fields string) {
	client := newClient()

	req, err := http.NewRequest(http.MethodGet, "http://sht/refs", nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(os.Stderr, resp.Body)
		os.Exit(1)
	}

	var out ListRefsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		die("bad response: %v", err)
	}

	dimDirty := stdoutIsTerminal()
	for _, ref := range out.Refs {
		if strings.ContainsRune(fields, 'a') {
			printRefLine(ref, fmt.Sprintf("%s %d %d %s %s", ref.Digest, ref.Size, ref.KeyID, ref.CreatedAt, refState(ref)), dimDirty)
			continue
		}

		var values []string
		for _, field := range fields {
			switch field {
			case 'd':
				values = append(values, ref.Digest)
			case 's':
				values = append(values, strconv.FormatInt(ref.Size, 10))
			case 'k':
				values = append(values, strconv.FormatInt(ref.KeyID, 10))
			case 'c':
				values = append(values, ref.CreatedAt)
			case 't':
				values = append(values, refState(ref))
			}
		}
		printRefLine(ref, strings.Join(values, " "), dimDirty)
	}
}

func doUploadRequest(req *http.Request, out any) {
	client := newClient()
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(os.Stderr, resp.Body)
		os.Exit(1)
	}

	if out == nil {
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		die("bad response: %v", err)
	}
}

func printJSON(value any) {
	data, err := json.Marshal(value)
	if err != nil {
		die("bad response: %v", err)
	}
	fmt.Println(string(data))
}

func manifest(keyID int64) {
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads", os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadChunk(keyID int64, args []string) {
	if len(args) != 2 {
		die("usage: sht upload-chunk <digest> <index>")
	}
	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || index < 0 {
		die("invalid chunk index")
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://sht/uploads/%s/chunks/%d", args[0], index), os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out ChunkUploadResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadStatus(keyID int64, args []string) {
	if len(args) != 1 {
		die("usage: sht upload-status <digest>")
	}
	req, err := http.NewRequest(http.MethodGet, "http://sht/uploads/"+args[0], nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func finalize(keyID int64, args []string) {
	if len(args) != 1 {
		die("usage: sht finalize <digest>")
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads/"+args[0]+"/finalize", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out StoreBlobResponse
	doUploadRequest(req, &out)
	fmt.Println(out.Digest)
}

func commandArgs() []string {
	return strings.Fields(os.Getenv("SSH_ORIGINAL_COMMAND"))
}

func main() {
	if len(os.Args) != 3 || os.Args[1] != "id" {
		for i, arg := range os.Args {
			if i == 0 {
				fmt.Print("malformed command: ", arg)
				continue
			}
			fmt.Print(" ", arg)
		}
		fmt.Println()
		fmt.Println("	1. use command=\"sht id <key id>\" for each key in authorized_keys")
		die("	2. don't use ForceCommand in sshd_config")
	}

	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || id <= 0 {
		die("invalid key id")
	}

	args := commandArgs()

	if len(args) == 0 {
		store(id)
		return
	}

	switch args[0] {
	case "stat":
		stat(collectDigests(args[1:], "stat"), id)
	case "cat":
		cat(collectDigests(args[1:], "cat"), id)
	case "release":
		release(collectDigests(args[1:], "release"), id)
	case "list", "refs":
		refs(id, parseListFields(args[1:]))
	case "manifest":
		if len(args) != 1 {
			die("usage: sht manifest < manifest.json")
		}
		manifest(id)
	case "upload-chunk":
		uploadChunk(id, args[1:])
	case "upload-status":
		uploadStatus(id, args[1:])
	case "finalize":
		finalize(id, args[1:])
	case "help", "-h", "--help":
		usage()
	default:
		die("unknown command: %s", args[0])
	}
}
