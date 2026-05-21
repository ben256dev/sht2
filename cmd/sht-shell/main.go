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
	sockDir  = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  sht               # upload stdin")
	fmt.Fprintln(os.Stderr, "  sht cat  <digest> # print blob")
	fmt.Fprintln(os.Stderr, "  sht stat <digest> # stat blob")
	fmt.Fprintln(os.Stderr, "  sht help          # show this message")
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

func store(keyID int64) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}

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

func cat(digest string, keyID int64) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}

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
		io.Copy(os.Stderr, resp.Body)
		os.Exit(1)
	}

	io.Copy(os.Stdout, resp.Body)
}

func stat(digest string, keyID int64) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}

	req, err := http.NewRequest(http.MethodHead, "http://sht/blob/"+digest, nil)
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
		if resp.StatusCode == http.StatusNotFound {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, resp.Status)
		os.Exit(1)
	}

	fmt.Println("exists")
}

func commandArgs() []string {
	if s := os.Getenv("SSH_ORIGINAL_COMMAND"); s != "" {
		return strings.Fields(s)
	}
	return os.Args[1:]
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

	fmt.Println("key id:", id)

	args := commandArgs()

	if len(args) == 0 {
		store(id)
		return
	}

	switch args[0] {
	case "stat":
		if len(args) != 2 {
			die("usage: sht stat <digest>")
		}
		stat(args[1], id)
	case "cat":
		if len(args) != 2 {
			die("usage: sht cat <digest>")
		}
		cat(args[1], id)
	case "help", "-h", "--help":
		usage()
	default:
		die("unknown command: %s", args[0])
	}
}
