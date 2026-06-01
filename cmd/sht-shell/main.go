package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
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
	fmt.Fprintln(os.Stderr, "  sht                       upload stdin to default shelf")
	fmt.Fprintln(os.Stderr, "  sht <shelf>               upload stdin to shelf")
	fmt.Fprintln(os.Stderr, "  sht <command> [args...]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  cat [digest ...]          print blobs")
	fmt.Fprintln(os.Stderr, "  stat [digest ...]         show blob status")
	fmt.Fprintln(os.Stderr, "  release [digest ...]      release blobs")
	fmt.Fprintln(os.Stderr, "  list [fields]             list refs")
	fmt.Fprintln(os.Stderr, "  quota                     show quota usage")
	fmt.Fprintln(os.Stderr, "  shelf <command>           manage shelves")
	fmt.Fprintln(os.Stderr, "  upload <command>          resumable uploads")
	fmt.Fprintln(os.Stderr, "  help [topic]              show help")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "digest commands read digests from stdin when none are given.")
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

func shelfUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht shelf list")
	fmt.Fprintln(os.Stderr, "  sht shelf create <name> <max> [pending-max]")
	fmt.Fprintln(os.Stderr, "  sht shelf rename <old> <new>")
	fmt.Fprintln(os.Stderr, "  sht shelf default <name>")
	fmt.Fprintln(os.Stderr, "  sht shelf delete <name> --force")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Creating a non-default shelf enables multi-shelf mode.")
	fmt.Fprintln(os.Stderr, "After that, scoped commands use the shelf name as the first word:")
	fmt.Fprintln(os.Stderr, "  sht <shelf>")
	fmt.Fprintln(os.Stderr, "  sht <shelf> list [fields]")
	fmt.Fprintln(os.Stderr, "  sht <shelf> release [digest ...]")
}

func listUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht list [fields]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "fields:")
	fmt.Fprintln(os.Stderr, "  d  digest")
	fmt.Fprintln(os.Stderr, "  h  shelf")
	fmt.Fprintln(os.Stderr, "  s  size")
	fmt.Fprintln(os.Stderr, "  k  key id")
	fmt.Fprintln(os.Stderr, "  c  created at")
	fmt.Fprintln(os.Stderr, "  t  state")
	fmt.Fprintln(os.Stderr, "  a  all (default)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "examples:")
	fmt.Fprintln(os.Stderr, "  sht list")
	fmt.Fprintln(os.Stderr, "  sht list d")
	fmt.Fprintln(os.Stderr, "  sht list dht")
	fmt.Fprintln(os.Stderr, "  sht <shelf> list dt")
}

func uploadUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht manifest              create/resume upload from manifest JSON")
	fmt.Fprintln(os.Stderr, "  sht upload <id> <index>   upload chunk bytes")
	fmt.Fprintln(os.Stderr, "  sht status <id>           show upload status")
	fmt.Fprintln(os.Stderr, "  sht finalize <id>         finalize upload")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Pipe the manifest JSON to stdin for 'manifest'.")
	fmt.Fprintln(os.Stderr, "Pipe chunk bytes to stdin for 'upload'.")
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
	Shelf     string `json:"shelf"`
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

type Shelf struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	MaxBytes        int64  `json:"max_bytes"`
	UsedBytes       int64  `json:"used_bytes"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
	PendingBytes    int64  `json:"pending_bytes"`
	RefCount        int64  `json:"ref_count"`
	Enabled         bool   `json:"enabled"`
	IsDefault       bool   `json:"is_default"`
}

type ShelfListResponse struct {
	Shelves []Shelf `json:"shelves"`
}

type QuotaResponse struct {
	UsedBytes           int64 `json:"used_bytes"`
	MaxBytes            int64 `json:"max_bytes"`
	PendingBytes        int64 `json:"pending_bytes"`
	MaxPendingBytes     int64 `json:"max_pending_bytes"`
	UploadReservedBytes int64 `json:"upload_reserved_bytes"`
	CleanDigestCount    int64 `json:"clean_digest_count"`
}

type ShelfCreateRequest struct {
	Name            string `json:"name"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
}

type ShelfRenameRequest struct {
	Name string `json:"name"`
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

func setShelfHeader(req *http.Request, shelf string) {
	if shelf != "" {
		req.Header.Set("X-SHT-Shelf", shelf)
	}
}

func store(keyID int64, shelf string) {
	client := newClient()

	req, err := http.NewRequest(http.MethodPost, "http://sht/blob", os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

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
	fmt.Println()
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

func releaseOne(client *http.Client, digest string, keyID int64, shelf string) (int, error) {
	req, err := http.NewRequest(http.MethodDelete, "http://sht/blob/"+digest, nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func release(digests []string, keyID int64, shelf string) {
	client := newClient()
	multiple := len(digests) > 1
	ok := true

	for _, digest := range digests {
		code, err := releaseOne(client, digest, keyID, shelf)
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
	if len(args) != 1 {
		die("usage: sht list [fields]")
	}

	fields := strings.TrimPrefix(args[0], "-")
	if fields == "" {
		die("usage: sht list [fields]")
	}
	for _, field := range fields {
		switch field {
		case 'd', 'h', 's', 'k', 'c', 't', 'a':
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

func refFieldValues(ref BlobRef, fields string) []string {
	if strings.ContainsRune(fields, 'a') {
		return []string{
			ref.Digest,
			ref.Shelf,
			strconv.FormatInt(ref.Size, 10),
			strconv.FormatInt(ref.KeyID, 10),
			ref.CreatedAt,
			refState(ref),
		}
	}

	var values []string
	for _, field := range fields {
		switch field {
		case 'd':
			values = append(values, ref.Digest)
		case 'h':
			values = append(values, ref.Shelf)
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
	return values
}

func formatRefRows(refs []BlobRef, fields string) []string {
	rows := make([][]string, 0, len(refs))
	var widths []int
	for _, ref := range refs {
		values := refFieldValues(ref, fields)
		if widths == nil {
			widths = make([]int, len(values))
		}
		for i, value := range values {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
		rows = append(rows, values)
	}

	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		var parts []string
		for i, value := range row {
			if i == len(row)-1 {
				parts = append(parts, value)
				continue
			}
			parts = append(parts, fmt.Sprintf("%-*s", widths[i], value))
		}
		lines = append(lines, strings.Join(parts, "  "))
	}
	return lines
}

func refs(keyID int64, fields string, shelf string) {
	client := newClient()

	req, err := http.NewRequest(http.MethodGet, "http://sht/refs", nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

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
	lines := formatRefRows(out.Refs, fields)
	for i, ref := range out.Refs {
		printRefLine(ref, lines[i], dimDirty)
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

func manifest(keyID int64, shelf string) {
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads", os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)
	req.Header.Set("Content-Type", "application/json")

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadChunk(keyID int64, args []string, shelf string) {
	if len(args) != 2 {
		die("usage: sht upload <digest> <index>")
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
	setShelfHeader(req, shelf)

	var out ChunkUploadResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadStatus(keyID int64, args []string, shelf string) {
	if len(args) != 1 {
		die("usage: sht status <digest>")
	}
	req, err := http.NewRequest(http.MethodGet, "http://sht/uploads/"+args[0], nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func finalize(keyID int64, args []string, shelf string) {
	if len(args) != 1 {
		die("usage: sht finalize <digest>")
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads/"+args[0]+"/finalize", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	var out StoreBlobResponse
	doUploadRequest(req, &out)
	fmt.Println(out.Digest)
}

func quota(keyID int64, args []string) {
	if len(args) != 0 {
		die("usage: sht quota")
	}
	req, err := http.NewRequest(http.MethodGet, "http://sht/quota", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out QuotaResponse
	doUploadRequest(req, &out)
	rows := [][]string{
		{"live", fmt.Sprintf("%d/%d", out.UsedBytes, out.MaxBytes)},
		{"pending", fmt.Sprintf("%d/%d", out.PendingBytes, out.MaxPendingBytes)},
		{"reserved", strconv.FormatInt(out.UploadReservedBytes, 10)},
		{"clean", strconv.FormatInt(out.CleanDigestCount, 10)},
	}
	width := 0
	for _, row := range rows {
		if len(row[0]) > width {
			width = len(row[0])
		}
	}
	for _, row := range rows {
		fmt.Printf("%-*s  %s\n", width, row[0], row[1])
	}
}

func shelfList(keyID int64) {
	req, err := http.NewRequest(http.MethodGet, "http://sht/shelves", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out ShelfListResponse
	doUploadRequest(req, &out)
	rows := make([][]string, 0, len(out.Shelves))
	widths := make([]int, 6)
	for _, shelf := range out.Shelves {
		state := "disabled"
		if shelf.Enabled {
			state = "enabled"
		}
		defaultMarker := ""
		if shelf.IsDefault {
			defaultMarker = "default"
		}
		row := []string{
			shelf.Name,
			fmt.Sprintf("%d/%d", shelf.UsedBytes, shelf.MaxBytes),
			fmt.Sprintf("%d/%d", shelf.PendingBytes, shelf.MaxPendingBytes),
			strconv.FormatInt(shelf.RefCount, 10),
			state,
			defaultMarker,
		}
		for i, value := range row {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
		rows = append(rows, row)
	}
	for _, row := range rows {
		fmt.Printf("%-*s  %*s  %*s  %*s  %-*s", widths[0], row[0], widths[1], row[1], widths[2], row[2], widths[3], row[3], widths[4], row[4])
		if row[5] != "" {
			fmt.Printf("  %s", row[5])
		}
		fmt.Println()
	}
}

func shelfCreate(keyID int64, args []string) {
	if len(args) != 3 {
		die("usage: sht shelf create <name> <max_bytes> <max_pending_bytes>")
	}
	maxBytes, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || maxBytes <= 0 {
		die("invalid max_bytes")
	}
	maxPendingBytes, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || maxPendingBytes <= 0 {
		die("invalid max_pending_bytes")
	}
	body, err := json.Marshal(ShelfCreateRequest{
		Name:            args[0],
		MaxBytes:        maxBytes,
		MaxPendingBytes: maxPendingBytes,
	})
	if err != nil {
		log.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/shelves", strings.NewReader(string(body)))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out Shelf
	doUploadRequest(req, &out)
	fmt.Printf("%s %d %d enabled\n", out.Name, out.MaxBytes, out.MaxPendingBytes)
}

func shelfRename(keyID int64, args []string) {
	if len(args) != 2 {
		die("usage: sht shelf rename <old> <new>")
	}
	body, err := json.Marshal(ShelfRenameRequest{Name: args[1]})
	if err != nil {
		log.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPatch, "http://sht/shelves/"+url.PathEscape(args[0]), strings.NewReader(string(body)))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out Shelf
	doUploadRequest(req, &out)
	state := "disabled"
	if out.Enabled {
		state = "enabled"
	}
	if out.IsDefault {
		fmt.Printf("%s %d %d %s default\n", out.Name, out.MaxBytes, out.MaxPendingBytes, state)
		return
	}
	fmt.Printf("%s %d %d %s\n", out.Name, out.MaxBytes, out.MaxPendingBytes, state)
}

func shelfSetDefault(keyID int64, args []string) {
	if len(args) != 1 {
		die("usage: sht shelf default <name>")
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/shelves/"+url.PathEscape(args[0])+"/default", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out Shelf
	doUploadRequest(req, &out)
	fmt.Printf("%s default\n", out.Name)
}

func shelfDelete(keyID int64, args []string) {
	if len(args) != 2 || (args[1] != "--force" && args[1] != "-f") {
		die("usage: sht shelf delete <name> --force")
	}
	req, err := http.NewRequest(http.MethodDelete, "http://sht/shelves/"+url.PathEscape(args[0])+"?force=1", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	doUploadRequest(req, nil)
	fmt.Println("deleted")
}

func shelfCommand(keyID int64, args []string) {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "-h" || args[0] == "--help")) {
		shelfUsage()
		if len(args) == 0 {
			os.Exit(1)
		}
		os.Exit(0)
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			die("usage: sht shelf list")
		}
		shelfList(keyID)
	case "create":
		shelfCreate(keyID, args[1:])
	case "rename":
		shelfRename(keyID, args[1:])
	case "default", "set-default":
		shelfSetDefault(keyID, args[1:])
	case "delete":
		shelfDelete(keyID, args[1:])
	default:
		die("usage: sht shelf list|create|rename|default|delete")
	}
}

func commandArgs() []string {
	return strings.Fields(os.Getenv("SSH_ORIGINAL_COMMAND"))
}

func isCommand(arg string) bool {
	switch arg {
	case "stat", "cat", "release", "list", "refs", "quota", "manifest", "upload", "upload-chunk", "status", "upload-status", "finalize", "help", "-h", "--help", "shelf", "create", "rename", "set-default", "default", "delete":
		return true
	default:
		return false
	}
}

func scopedCommand(args []string, id int64, shelf string) {
	if len(args) == 0 {
		store(id, shelf)
		return
	}
	switch args[0] {
	case "release":
		release(collectDigests(args[1:], "release"), id, shelf)
	case "list", "refs":
		refs(id, parseListFields(args[1:]), shelf)
	case "manifest":
		if len(args) != 1 {
			die("usage: sht manifest < manifest.json")
		}
		manifest(id, shelf)
	case "upload", "upload-chunk":
		uploadChunk(id, args[1:], shelf)
	case "status", "upload-status":
		uploadStatus(id, args[1:], shelf)
	case "finalize":
		finalize(id, args[1:], shelf)
	default:
		die("unknown scoped command: %s", args[0])
	}
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
		store(id, "")
		return
	}

	if !isCommand(args[0]) {
		scopedCommand(args[1:], id, args[0])
		return
	}

	switch args[0] {
	case "stat":
		stat(collectDigests(args[1:], "stat"), id)
	case "cat":
		cat(collectDigests(args[1:], "cat"), id)
	case "release":
		release(collectDigests(args[1:], "release"), id, "")
	case "list", "refs":
		refs(id, parseListFields(args[1:]), "")
	case "quota":
		quota(id, args[1:])
	case "shelf":
		shelfCommand(id, args[1:])
	case "manifest":
		if len(args) != 1 {
			die("usage: sht manifest < manifest.json")
		}
		manifest(id, "")
	case "upload", "upload-chunk":
		uploadChunk(id, args[1:], "")
	case "status", "upload-status":
		uploadStatus(id, args[1:], "")
	case "finalize":
		finalize(id, args[1:], "")
	case "help", "-h", "--help":
		if len(args) >= 2 {
			switch args[1] {
			case "cat":
				catUsage()
			case "stat":
				statUsage()
			case "release":
				releaseUsage()
			case "shelf":
				shelfUsage()
			case "list":
				listUsage()
			case "upload":
				uploadUsage()
			default:
				die("unknown help topic: %s", args[1])
			}
		} else {
			usage()
		}
	case "create", "rename", "set-default", "default", "delete":
		die("usage: sht shelf %s ...", args[0])
	default:
		die("unknown command: %s", args[0])
	}
}
