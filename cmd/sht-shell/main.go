package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	sockDir  = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
	jsonMode bool
)

func mainUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht                       upload stdin to default shelf",
		"  sht <shelf>               upload stdin to shelf",
		"  sht --                    upload stdin to default shelf",
		"  sht <shelf> --            upload stdin to shelf",
		"  sht <command> [args...]",
		"",
		"commands:",
		"  cat [digest ...]          print blobs",
		"  stat [digest ...]         show blob status",
		"  release [digest ...]      release blobs",
		"  list [fields]             list refs",
		"  quota                     show quota usage",
		"  shelf <command>           manage shelves",
		"  alias <command>           manage aliases",
		"  upload <command>          resumable uploads",
		"  help [topic]              show help",
		"",
		"digest commands read digests from stdin when none are given.",
	}, "\n")
}

func catUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht cat [digest ...]",
		"",
		"Print blobs for whitespace-delimited digests.",
		"When no digests are given as arguments, digests are read from stdin.",
	}, "\n")
}

func statUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht stat [digest ...]",
		"",
		"Check blobs for whitespace-delimited digests.",
		"When no digests are given as arguments, digests are read from stdin.",
	}, "\n")
}

func releaseUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht release [digest ...]",
		"",
		"Release refs for whitespace-delimited digests.",
		"When no digests are given as arguments, digests are read from stdin.",
	}, "\n")
}

func shelfUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht shelf list",
		"  sht shelf create <name> <max> [pending-max]",
		"  sht shelf rename <old> <new>",
		"  sht shelf default <name>",
		"  sht shelf delete <name> --force",
		"",
		"Creating a non-default shelf enables multi-shelf mode.",
		"After that, scoped commands use the shelf name as the first word:",
		"  sht <shelf>",
		"  sht <shelf> --",
		"  sht <shelf> list [fields]",
		"  sht <shelf> release [digest ...]",
	}, "\n")
}

func listUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht list [fields]",
		"",
		"fields:",
		"  d  digest",
		"  h  shelf",
		"  s  size",
		"  k  key id",
		"  c  created at",
		"  t  state",
		"  a  all (default)",
		"",
		"examples:",
		"  sht list",
		"  sht list d",
		"  sht list dht",
		"  sht <shelf> list dt",
	}, "\n")
}

func uploadUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht manifest              create/resume upload from manifest JSON",
		"  sht upload <id> <index>   upload chunk bytes",
		"  sht status <id>           show upload status",
		"  sht finalize <id>         finalize upload",
		"",
		"Pipe the manifest JSON to stdin for 'manifest'.",
		"Pipe chunk bytes to stdin for 'upload'.",
	}, "\n")
}

func aliasUsageText() string {
	return strings.Join([]string{
		"usage:",
		"  sht alias ns list",
		"  sht alias ns create <name>",
		"  sht alias list <namespace> [prefix]",
		"  sht alias get <namespace> <path>",
		"  sht alias cat <namespace> <path>",
		"  sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]",
		"  sht alias history <namespace> <path>",
		"  sht alias grant <namespace> <path> <user> <read|write|admin>",
		"  sht alias revoke <namespace> <path> <user>",
		"",
		"Aliases are versioned names for blobs. Grants apply recursively to a directory path.",
		"Use / as the path when granting or revoking namespace-wide access.",
	}, "\n")
}

func usage() {
	printUsage("main", mainUsageText())
}

func catUsage() {
	printUsage("cat", catUsageText())
}

func statUsage() {
	printUsage("stat", statUsageText())
}

func releaseUsage() {
	printUsage("release", releaseUsageText())
}

func shelfUsage() {
	printUsage("shelf", shelfUsageText())
}

func listUsage() {
	printUsage("list", listUsageText())
}

func uploadUsage() {
	printUsage("upload", uploadUsageText())
}

func aliasUsage() {
	printUsage("alias", aliasUsageText())
}

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

type ShelfDeleteResponse struct {
	Deleted bool   `json:"deleted"`
	Name    string `json:"name"`
}

type AliasNamespace struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	OwnerID int64  `json:"owner_user_id"`
}

type AliasNamespaceListResponse struct {
	Namespaces []AliasNamespace `json:"namespaces"`
}

type AliasNamespaceCreateRequest struct {
	Name string `json:"name"`
}

type AliasGrant struct {
	Namespace string `json:"namespace,omitempty"`
	Path      string `json:"path"`
	User      string `json:"user"`
	Role      string `json:"role"`
}

type AliasGrantRequest struct {
	Path string `json:"path"`
	User string `json:"user"`
	Role string `json:"role,omitempty"`
}

type AliasGrantResponse struct {
	Grant AliasGrant `json:"grant"`
}

type AliasRevokeResponse struct {
	Revoked   bool   `json:"revoked"`
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
	User      string `json:"user"`
}

type Alias struct {
	Namespace        string `json:"namespace"`
	Path             string `json:"path"`
	Digest           string `json:"digest"`
	CurrentVersionID int64  `json:"current_version_id"`
	UpdatedAt        string `json:"updated_at"`
}

type AliasListResponse struct {
	Aliases []Alias `json:"aliases"`
}

type AliasVersion struct {
	ID                int64  `json:"id"`
	Namespace         string `json:"namespace"`
	Path              string `json:"path"`
	Digest            string `json:"digest"`
	AuthorUserID      int64  `json:"author_user_id"`
	AuthorUser        string `json:"author_user"`
	PreviousVersionID *int64 `json:"previous_version_id,omitempty"`
	Message           string `json:"message,omitempty"`
	CreatedAt         string `json:"created_at"`
}

type AliasVersionListResponse struct {
	Versions []AliasVersion `json:"versions"`
}

type AliasVersionCreateRequest struct {
	Digest          string `json:"digest"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
	Message         string `json:"message,omitempty"`
}

type AliasConflictResponse struct {
	Error          string `json:"error"`
	CurrentVersion int64  `json:"current_version"`
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
		die("usage: sht upload <digest> <index>")
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
		die("usage: sht status <digest>")
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
		die("usage: sht finalize <digest>")
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

func quota(keyID int64, args []string) {
	if len(args) != 0 {
		die("usage: sht quota")
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
		die("usage: sht shelf create <name> <max> [pending-max]")
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
		die("usage: sht shelf rename <old> <new>")
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
		die("usage: sht shelf default <name>")
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
		die("usage: sht shelf delete <name> --force")
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

func escapeAliasPath(path string) string {
	path = strings.Trim(path, "/")
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func aliasPathURL(namespace, path string, suffix ...string) string {
	u := "http://sht/alias/" + url.PathEscape(namespace)
	if escaped := escapeAliasPath(path); escaped != "" {
		u += "/" + escaped
	}
	for _, part := range suffix {
		u += "/" + url.PathEscape(part)
	}
	return u
}

func printAliasRows(aliases []Alias) {
	rows := make([][]string, 0, len(aliases))
	widths := make([]int, 5)
	for _, alias := range aliases {
		row := []string{
			alias.Namespace,
			alias.Path,
			alias.Digest,
			strconv.FormatInt(alias.CurrentVersionID, 10),
			alias.UpdatedAt,
		}
		for i, value := range row {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
		rows = append(rows, row)
	}
	for _, row := range rows {
		fmt.Printf("%-*s  %-*s  %-*s  %*s  %s\n", widths[0], row[0], widths[1], row[1], widths[2], row[2], widths[3], row[3], row[4])
	}
}

func aliasNamespaceList(keyID int64) {
	req, err := http.NewRequest(http.MethodGet, "http://sht/alias/namespaces", nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out AliasNamespaceListResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	for _, ns := range out.Namespaces {
		fmt.Printf("%s %d\n", ns.Name, ns.OwnerID)
	}
}

func aliasNamespaceCreate(keyID int64, args []string) {
	if len(args) != 1 {
		die("usage: sht alias ns create <name>")
	}
	body, err := json.Marshal(AliasNamespaceCreateRequest{Name: args[0]})
	failIfErr(err)
	req, err := http.NewRequest(http.MethodPost, "http://sht/alias/namespaces", strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out AliasNamespace
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Printf("%s %d\n", out.Name, out.OwnerID)
}

func aliasList(keyID int64, args []string) {
	if len(args) != 1 && len(args) != 2 {
		die("usage: sht alias list <namespace> [prefix]")
	}
	u := "http://sht/alias/" + url.PathEscape(args[0])
	if len(args) == 2 {
		q := url.Values{}
		q.Set("prefix", args[1])
		u += "?" + q.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out AliasListResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	printAliasRows(out.Aliases)
}

func aliasGet(keyID int64, args []string) {
	if len(args) != 2 {
		die("usage: sht alias get <namespace> <path>")
	}
	req, err := http.NewRequest(http.MethodGet, aliasPathURL(args[0], args[1]), nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out Alias
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	printAliasRows([]Alias{out})
}

func aliasCatJSON(keyID int64, namespace, path string) CatResult {
	metaReq, err := http.NewRequest(http.MethodGet, aliasPathURL(namespace, path), nil)
	failIfErr(err)
	metaReq.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	var alias Alias
	doUploadRequest(metaReq, &alias)

	req, err := http.NewRequest(http.MethodGet, aliasPathURL(namespace, path, "blob"), nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	client := newClient()
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
		return CatResult{Digest: alias.Digest, Error: &BlobResultError{Message: message, Status: resp.StatusCode}}
	}
	return CatResult{Digest: alias.Digest, Size: int64(len(data)), ContentBase64: base64.StdEncoding.EncodeToString(data)}
}

func aliasCat(keyID int64, args []string) {
	if len(args) != 2 {
		die("usage: sht alias cat <namespace> <path>")
	}
	if jsonMode {
		printJSON(CatResponse{Blobs: []CatResult{aliasCatJSON(keyID, args[0], args[1])}})
		return
	}
	req, err := http.NewRequest(http.MethodGet, aliasPathURL(args[0], args[1], "blob"), nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	client := newClient()
	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		handleHTTPError(resp)
	}
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func aliasSet(keyID int64, args []string) {
	if len(args) < 3 {
		die("usage: sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]")
	}
	namespace := args[0]
	path := args[1]
	reqBody := AliasVersionCreateRequest{Digest: args[2]}
	for i := 3; i < len(args); i++ {
		switch args[i] {
		case "--expect", "-e":
			if i+1 >= len(args) {
				die("usage: sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]")
			}
			version, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || version < 0 {
				die("invalid expected version")
			}
			reqBody.ExpectedVersion = &version
			i++
		case "--message", "-m":
			if i+1 >= len(args) {
				die("usage: sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]")
			}
			j := i + 1
			for j < len(args) && args[j] != "--expect" && args[j] != "-e" && args[j] != "--message" && args[j] != "-m" {
				j++
			}
			if j == i+1 {
				die("usage: sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]")
			}
			reqBody.Message = trimPairedQuotes(strings.Join(args[i+1:j], " "))
			i = j - 1
		default:
			die("unknown alias set option: %s", args[i])
		}
	}
	body, err := json.Marshal(reqBody)
	failIfErr(err)
	req, err := http.NewRequest(http.MethodPost, aliasPathURL(namespace, path, "versions"), strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	client := newClient()
	resp, err := client.Do(req)
	failIfErr(err)
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, readErr := io.ReadAll(resp.Body)
		failIfErr(readErr)
		if jsonMode {
			message := strings.TrimSpace(string(data))
			if resp.StatusCode == http.StatusConflict && json.Valid(data) {
				fmt.Fprintln(os.Stderr, message)
			} else {
				if message == "" {
					message = http.StatusText(resp.StatusCode)
				}
				printJSONError(message, http.StatusText(resp.StatusCode), resp.StatusCode)
			}
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusConflict {
			var conflict AliasConflictResponse
			_ = json.Unmarshal(data, &conflict)
			current, err := aliasCurrent(keyID, namespace, path)
			if err == nil {
				if current.Digest == reqBody.Digest {
					fmt.Println("up to date")
					return
				}
				fmt.Printf("stale alias version: current is %d\n", current.CurrentVersionID)
				os.Exit(1)
			}
			if conflict.CurrentVersion > 0 {
				fmt.Printf("stale alias version: current is %d\n", conflict.CurrentVersion)
				os.Exit(1)
			}
		}
		message := strings.TrimSpace(string(data))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		fmt.Fprintln(os.Stderr, message)
		os.Exit(1)
	}

	var out AliasVersion
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		die("bad response: %v", err)
	}
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Printf("%s %s %d\n", out.Path, out.Digest, out.ID)
}

func trimPairedQuotes(value string) string {
	if len(value) < 2 {
		return value
	}
	if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
		return value[1 : len(value)-1]
	}
	return value
}

func aliasCurrent(keyID int64, namespace, path string) (Alias, error) {
	req, err := http.NewRequest(http.MethodGet, aliasPathURL(namespace, path), nil)
	if err != nil {
		return Alias{}, err
	}
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	client := newClient()
	resp, err := client.Do(req)
	if err != nil {
		return Alias{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Alias{}, fmt.Errorf("%s", httpErrorMessage(resp))
	}
	var out Alias
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Alias{}, err
	}
	return out, nil
}

func aliasHistory(keyID int64, args []string) {
	if len(args) != 2 {
		die("usage: sht alias history <namespace> <path>")
	}
	req, err := http.NewRequest(http.MethodGet, aliasPathURL(args[0], args[1], "versions"), nil)
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))

	var out AliasVersionListResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	rows := make([][]string, 0, len(out.Versions))
	widths := make([]int, 5)
	for _, version := range out.Versions {
		row := []string{
			strconv.FormatInt(version.ID, 10),
			version.Digest,
			version.AuthorUser,
			version.CreatedAt,
			version.Message,
		}
		for i, value := range row {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
		rows = append(rows, row)
	}
	for _, row := range rows {
		fmt.Printf("%*s  %-*s  %-*s  %-*s", widths[0], row[0], widths[1], row[1], widths[2], row[2], widths[3], row[3])
		if row[4] != "" {
			fmt.Printf("  %s", row[4])
		}
		fmt.Println()
	}
}

func aliasGrant(keyID int64, args []string) {
	if len(args) != 4 {
		die("usage: sht alias grant <namespace> <path> <user> <read|write|admin>")
	}
	body, err := json.Marshal(AliasGrantRequest{Path: args[1], User: args[2], Role: args[3]})
	failIfErr(err)
	req, err := http.NewRequest(http.MethodPost, "http://sht/alias/"+url.PathEscape(args[0])+"/grants", strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	var out AliasGrantResponse
	doUploadRequest(req, &out)
	if jsonMode {
		printJSON(out)
		return
	}
	fmt.Printf("%s %s %s\n", out.Grant.Path, out.Grant.User, out.Grant.Role)
}

func aliasRevoke(keyID int64, args []string) {
	if len(args) != 3 {
		die("usage: sht alias revoke <namespace> <path> <user>")
	}
	body, err := json.Marshal(AliasGrantRequest{Path: args[1], User: args[2]})
	failIfErr(err)
	req, err := http.NewRequest(http.MethodDelete, "http://sht/alias/"+url.PathEscape(args[0])+"/grants", strings.NewReader(string(body)))
	failIfErr(err)
	req.Header.Set("X-SHT-Key-ID", strconv.FormatInt(keyID, 10))
	req.Header.Set("Content-Type", "application/json")

	doUploadRequest(req, nil)
	if jsonMode {
		printJSON(AliasRevokeResponse{Revoked: true, Namespace: args[0], Path: args[1], User: args[2]})
		return
	}
	fmt.Println("revoked")
}

func aliasCommand(keyID int64, args []string) {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "-h" || args[0] == "--help")) {
		aliasUsage()
		if len(args) == 0 {
			os.Exit(1)
		}
		os.Exit(0)
	}
	switch args[0] {
	case "ns", "namespace", "namespaces":
		if len(args) < 2 {
			die("usage: sht alias ns list|create")
		}
		switch args[1] {
		case "list":
			if len(args) != 2 {
				die("usage: sht alias ns list")
			}
			aliasNamespaceList(keyID)
		case "create":
			aliasNamespaceCreate(keyID, args[2:])
		default:
			die("usage: sht alias ns list|create")
		}
	case "list":
		aliasList(keyID, args[1:])
	case "get":
		aliasGet(keyID, args[1:])
	case "cat":
		aliasCat(keyID, args[1:])
	case "set":
		aliasSet(keyID, args[1:])
	case "history", "log":
		aliasHistory(keyID, args[1:])
	case "grant":
		aliasGrant(keyID, args[1:])
	case "revoke":
		aliasRevoke(keyID, args[1:])
	default:
		die("usage: sht alias ns|list|get|cat|set|history|grant|revoke")
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
			die("usage: sht <shelf> --")
		}
		store(id, shelf)
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
			die("usage: sht --")
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
		die("usage: sht shelf default ...")
	case "create", "rename", "delete":
		die("usage: sht shelf %s ...", args[0])
	default:
		die("unknown command: %s", args[0])
	}
}
