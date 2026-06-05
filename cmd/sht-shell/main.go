package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	jsonMode bool
)

func die(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if jsonMode {
		printJSONError(message, "error", 0)
	} else {
		fmt.Fprintln(os.Stderr, message)
	}
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

type QuotaResponse struct {
	UsedBytes           int64 `json:"used_bytes"`
	MaxBytes            int64 `json:"max_bytes"`
	PendingBytes        int64 `json:"pending_bytes"`
	MaxPendingBytes     int64 `json:"max_pending_bytes"`
	UploadReservedBytes int64 `json:"upload_reserved_bytes"`
	CleanDigestCount    int64 `json:"clean_digest_count"`
}

type ErrorDetail struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	Status  int    `json:"status,omitempty"`
}

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type UsageResponse struct {
	Topic string `json:"topic"`
	Usage string `json:"usage"`
}

type BlobResultError struct {
	Message string `json:"message"`
	Status  int    `json:"status,omitempty"`
}

type CatResult struct {
	Digest        string           `json:"digest"`
	Size          int64            `json:"size,omitempty"`
	ContentBase64 string           `json:"content_base64,omitempty"`
	Error         *BlobResultError `json:"error,omitempty"`
}

type CatResponse struct {
	Blobs []CatResult `json:"blobs"`
}

type StatResult struct {
	Digest string           `json:"digest"`
	Exists bool             `json:"exists"`
	Status int              `json:"status"`
	Error  *BlobResultError `json:"error,omitempty"`
}

type StatResponse struct {
	Results []StatResult `json:"results"`
}

type ReleaseResult struct {
	Digest   string           `json:"digest"`
	Released bool             `json:"released"`
	Status   int              `json:"status"`
	Error    *BlobResultError `json:"error,omitempty"`
}

type ReleaseResponse struct {
	Results []ReleaseResult `json:"results"`
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

func printJSONTo(w io.Writer, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"error":{"message":"bad json response: %s","code":"error"}}`+"\n", err.Error())
		os.Exit(1)
	}
	fmt.Fprintln(w, string(data))
}

func printJSONError(message, code string, status int) {
	message = strings.TrimSpace(message)
	if message == "" && status > 0 {
		message = http.StatusText(status)
	}
	if code == "" {
		code = "error"
	}
	printJSONTo(os.Stderr, ErrorResponse{Error: ErrorDetail{
		Message: message,
		Code:    code,
		Status:  status,
	}})
}

func printUsage(topic, text string) {
	if jsonMode {
		printJSONTo(os.Stdout, UsageResponse{Topic: topic, Usage: text})
		return
	}
	fmt.Fprintln(os.Stderr, text)
}

func failIfErr(err error) {
	if err != nil {
		die("%v", err)
	}
}

func splitJSONFlag(args []string) ([]string, bool) {
	filtered := make([]string, 0, len(args))
	found := false
	for _, arg := range args {
		if arg == "-j" {
			found = true
			continue
		}
		filtered = append(filtered, arg)
	}
	return filtered, found
}

func httpErrorMessage(resp *http.Response) string {
	data, _ := io.ReadAll(resp.Body)
	message := strings.TrimSpace(string(data))
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return message
}

func handleHTTPError(resp *http.Response) {
	message := httpErrorMessage(resp)
	if jsonMode {
		printJSONError(message, http.StatusText(resp.StatusCode), resp.StatusCode)
	} else {
		fmt.Fprintln(os.Stderr, message)
	}
	os.Exit(1)
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
		switch command {
		case "cat":
			die(usageCatDigests)
		case "stat":
			die(usageStatDigests)
		case "release":
			die(usageReleaseDigests)
		default:
			die("usage: sht %s [digest ...]", command)
		}
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
	failIfErr(err)

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		handleHTTPError(resp)
	}

	var out StoreBlobResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		die("bad response: %v", err)
	}

	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Println(out.Digest)
}

func catOne(client *http.Client, digest string, keyID int64) bool {
	req, err := http.NewRequest(http.MethodGet, "http://sht/blob/"+digest, nil)
	failIfErr(err)

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%s: ", digest)
		io.Copy(os.Stderr, resp.Body)
		return false
	}

	io.Copy(os.Stdout, resp.Body)
	return true
}

func catOneJSON(client *http.Client, digest string, keyID int64) (CatResult, bool) {
	req, err := http.NewRequest(http.MethodGet, "http://sht/blob/"+digest, nil)
	failIfErr(err)

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	failIfErr(err)
	if resp.StatusCode != http.StatusOK {
		message := strings.TrimSpace(string(data))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return CatResult{
			Digest: digest,
			Error: &BlobResultError{
				Message: message,
				Status:  resp.StatusCode,
			},
		}, false
	}

	return CatResult{
		Digest:        digest,
		Size:          int64(len(data)),
		ContentBase64: base64.StdEncoding.EncodeToString(data),
	}, true
}

func cat(digests []string, keyID int64) {
	client := newClient()
	ok := true
	if jsonMode {
		results := make([]CatResult, 0, len(digests))
		for _, digest := range digests {
			result, resultOK := catOneJSON(client, digest, keyID)
			results = append(results, result)
			if !resultOK {
				ok = false
			}
		}
		printJSON(CatResponse{Blobs: results})
		if !ok {
			os.Exit(1)
		}
		return
	}

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
	if jsonMode {
		results := make([]StatResult, 0, len(digests))
		for _, digest := range digests {
			code, err := statOne(client, digest, keyID)
			failIfErr(err)
			result := StatResult{
				Digest: digest,
				Exists: code == http.StatusOK,
				Status: code,
			}
			if code != http.StatusOK {
				ok = false
				result.Error = &BlobResultError{
					Message: http.StatusText(code),
					Status:  code,
				}
			}
			results = append(results, result)
		}
		printJSON(StatResponse{Results: results})
		if !ok {
			os.Exit(1)
		}
		return
	}

	for _, digest := range digests {
		code, err := statOne(client, digest, keyID)
		failIfErr(err)
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

func releaseOne(client *http.Client, digest string, keyID int64, shelf string) (int, string, error) {
	req, err := http.NewRequest(http.MethodDelete, "http://sht/blob/"+digest, nil)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	message := ""
	if resp.StatusCode != http.StatusNoContent {
		message = httpErrorMessage(resp)
	}
	return resp.StatusCode, message, nil
}

func release(digests []string, keyID int64, shelf string) {
	client := newClient()
	multiple := len(digests) > 1
	ok := true
	if jsonMode {
		results := make([]ReleaseResult, 0, len(digests))
		for _, digest := range digests {
			code, message, err := releaseOne(client, digest, keyID, shelf)
			failIfErr(err)
			result := ReleaseResult{
				Digest:   digest,
				Released: code == http.StatusNoContent,
				Status:   code,
			}
			if code != http.StatusNoContent {
				ok = false
				if message == "" {
					message = http.StatusText(code)
				}
				result.Error = &BlobResultError{
					Message: message,
					Status:  code,
				}
			}
			results = append(results, result)
		}
		printJSON(ReleaseResponse{Results: results})
		if !ok {
			os.Exit(1)
		}
		return
	}

	for _, digest := range digests {
		code, message, err := releaseOne(client, digest, keyID, shelf)
		failIfErr(err)
		switch code {
		case http.StatusNoContent:
			if multiple {
				fmt.Println(digest, "released")
			} else {
				fmt.Println("released")
			}
		case http.StatusNotFound:
			if message == "" {
				message = "not found"
			}
			if multiple {
				fmt.Fprintln(os.Stderr, digest, message)
			} else {
				fmt.Fprintln(os.Stderr, message)
			}
			ok = false
		default:
			if message == "" {
				message = http.StatusText(code)
			}
			if multiple {
				fmt.Fprintln(os.Stderr, digest, message)
			} else {
				fmt.Fprintln(os.Stderr, message)
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
		die(usageListFields)
	}

	fields := strings.TrimPrefix(args[0], "-")
	if fields == "" {
		die(usageListFields)
	}
	if strings.ContainsRune(fields, 'h') {
		listUsage()
		os.Exit(0)
	}
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
	failIfErr(err)

	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	setShelfHeader(req, shelf)

	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		handleHTTPError(resp)
	}

	var out ListRefsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		die("bad response: %v", err)
	}

	if jsonMode {
		printJSON(out)
		return
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
	failIfErr(err)
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		handleHTTPError(resp)
	}

	if out == nil {
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		die("bad response: %v", err)
	}
}

func printJSON(value any) {
	printJSONTo(os.Stdout, value)
}

func quota(keyID int64, args []string) {
	if len(args) != 0 {
		die(usageQuota)
	}
	req, err := http.NewRequest(http.MethodGet, "http://sht/quota", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out QuotaResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
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

func commandArgs() ([]string, error) {
	input := os.Getenv("SSH_ORIGINAL_COMMAND")
	var args []string
	var b strings.Builder
	var quote rune
	inArg := false
	escaped := false

	for _, r := range input {
		if escaped {
			b.WriteRune(r)
			inArg = true
			escaped = false
			continue
		}
		if quote != '\'' && r == '\\' {
			escaped = true
			inArg = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
				inArg = true
				continue
			}
			b.WriteRune(r)
			inArg = true
			continue
		}
		switch r {
		case '\'', '"':
			quote = r
			inArg = true
		case ' ', '\t', '\n', '\r':
			if inArg {
				args = append(args, b.String())
				b.Reset()
				inArg = false
			}
		default:
			b.WriteRune(r)
			inArg = true
		}
	}
	if escaped {
		return nil, fmt.Errorf("unfinished escape")
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if inArg {
		args = append(args, b.String())
	}
	return args, nil
}

func isCommand(arg string) bool {
	switch arg {
	case "--", "stat", "cat", "release", "list", "refs", "quota", "manifest", "upload", "upload-chunk", "status", "upload-status", "finalize", "help", "-h", "--help", "shelf", "alias", "create", "rename", "set-default", "default", "delete":
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
	case "--":
		if len(args) != 1 {
			die(usageShelfScoped)
		}
		store(id, shelf)
	case "release":
		release(collectDigests(args[1:], "release"), id, shelf)
	case "list", "refs":
		refs(id, parseListFields(args[1:]), shelf)
	case "manifest":
		if len(args) != 1 {
			die(usageUploadManifest)
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
	args, err := commandArgs()
	if err != nil {
		jsonMode = strings.Contains(os.Getenv("SSH_ORIGINAL_COMMAND"), "-j")
		die("bad command: %v", err)
	}
	args, jsonMode = splitJSONFlag(args)

	if len(os.Args) != 3 || os.Args[1] != "id" {
		if jsonMode {
			die("malformed command: use command=\"sht id <key id>\" for each key in authorized_keys; don't use ForceCommand in sshd_config")
		}
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

	if len(args) == 0 {
		store(id, "")
		return
	}

	if !isCommand(args[0]) {
		scopedCommand(args[1:], id, args[0])
		return
	}

	switch args[0] {
	case "--":
		if len(args) != 1 {
			die(usageUploadStdin)
		}
		store(id, "")
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
	case "alias":
		aliasCommand(id, args[1:])
	case "manifest":
		if len(args) != 1 {
			die(usageUploadManifest)
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
			case "alias":
				aliasUsage()
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
	case "set-default", "default":
		die(usageShelfDefaultDo)
	case "create", "rename", "delete":
		die("usage: sht shelf %s ...", args[0])
	default:
		die("unknown command: %s", args[0])
	}
}
