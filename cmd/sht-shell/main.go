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

func store() {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}

	resp, err := client.Post(
		"http://sht/blob",
		"text/plain",
		os.Stdin,
	)
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

func cat(digest string) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", sockDir)
			},
		},
	}

	resp, err := client.Get(
		fmt.Sprintf("http://sht/blob/%s", digest),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(os.Stdout, resp.Body)
}

func stat(digest string) {
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
	args := commandArgs()

	if len(args) == 0 {
		store()
		return
	}

	switch args[0] {
	case "stat":
		if len(args) != 2 {
			die("usage: sht stat <digest>")
		}
		stat(args[1])
	case "cat":
		if len(args) != 2 {
			die("usage: sht cat <digest>")
		}
		cat(args[1])
	case "help", "-h", "--help":
		usage()
	default:
		die("unknown command: %s", args[0])
	}
}
