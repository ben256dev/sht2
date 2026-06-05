package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
)

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

func manifest(keyID int64, shelf string) {
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads", os.Stdin)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)
	req.Header.Set("Content-Type", "application/json")

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadChunk(keyID int64, args []string, shelf string) {
	if len(args) != 2 {
		die(usageUploadChunk)
	}
	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || index < 0 {
		die("invalid chunk index")
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://sht/uploads/%s/chunks/%d", args[0], index), os.Stdin)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	var out ChunkUploadResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func uploadStatus(keyID int64, args []string, shelf string) {
	if len(args) != 1 {
		die(usageUploadStatus)
	}
	req, err := http.NewRequest(http.MethodGet, "http://sht/uploads/"+args[0], nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	var out UploadStatusResponse
	doUploadRequest(req, &out)
	printJSON(out)
}

func finalize(keyID int64, args []string, shelf string) {
	if len(args) != 1 {
		die(usageUploadFinalize)
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/uploads/"+args[0]+"/finalize", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	var out StoreBlobResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Println(out.Digest)
}
