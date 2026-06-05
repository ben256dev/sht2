package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

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

type ShelfCreateRequest struct {
	Name            string `json:"name"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
}

type ShelfRenameRequest struct {
	Name string `json:"name"`
}

type ShelfDeleteResponse struct {
	Deleted bool   `json:"deleted"`
	Name    string `json:"name"`
}

func shelfList(keyID int64) {
	req, err := http.NewRequest(http.MethodGet, "http://sht/shelves", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out ShelfListResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
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
	if len(args) != 2 && len(args) != 3 {
		die(usageShelfCreate)
	}
	maxBytes, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || maxBytes <= 0 {
		die("invalid max_bytes")
	}
	maxPendingBytes := maxBytes * 5 / 4
	if len(args) == 3 {
		maxPendingBytes, err = strconv.ParseInt(args[2], 10, 64)
		if err != nil || maxPendingBytes <= 0 {
			die("invalid max_pending_bytes")
		}
	}
	body, err := json.Marshal(ShelfCreateRequest{
		Name:            args[0],
		MaxBytes:        maxBytes,
		MaxPendingBytes: maxPendingBytes,
	})
	failIfErr(err)
	req, err := http.NewRequest(http.MethodPost, "http://sht/shelves", strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out Shelf
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Printf("%s %d %d enabled\n", out.Name, out.MaxBytes, out.MaxPendingBytes)
}

func shelfRename(keyID int64, args []string) {
	if len(args) != 2 {
		die(usageShelfRename)
	}
	body, err := json.Marshal(ShelfRenameRequest{Name: args[1]})
	failIfErr(err)
	req, err := http.NewRequest(http.MethodPatch, "http://sht/shelves/"+url.PathEscape(args[0]), strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out Shelf
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
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
		die(usageShelfDefault)
	}
	req, err := http.NewRequest(http.MethodPost, "http://sht/shelves/"+url.PathEscape(args[0])+"/default", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out Shelf
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Printf("%s default\n", out.Name)
}

func shelfDelete(keyID int64, args []string) {
	if len(args) != 2 || (args[1] != "--force" && args[1] != "-f") {
		die(usageShelfDelete)
	}
	req, err := http.NewRequest(http.MethodDelete, "http://sht/shelves/"+url.PathEscape(args[0])+"?force=1", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	doUploadRequest(req, nil)
	if jsonMode {
		printJSON(ShelfDeleteResponse{Deleted: true, Name: args[0]})
		return
	}
	fmt.Println("deleted")
}

func shelfCommand(keyID int64, args []string) {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help")) {
		shelfUsage()
		if len(args) == 0 {
			os.Exit(1)
		}
		os.Exit(0)
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			die(usageShelfList)
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
		shelfUsage()
		os.Exit(1)
	}
}
