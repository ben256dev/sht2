package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

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
		die(usageAliasNSCreate)
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
		die(usageAliasList)
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
		die(usageAliasGet)
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
		die(usageAliasCat)
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
		die(usageAliasSet)
	}
	namespace := args[0]
	path := args[1]
	reqBody := AliasVersionCreateRequest{Digest: args[2]}
	for i := 3; i < len(args); i++ {
		switch args[i] {
		case "--expect", "-e":
			if i+1 >= len(args) {
				die(usageAliasSet)
			}
			version, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || version < 0 {
				die("invalid expected version")
			}
			reqBody.ExpectedVersion = &version
			i++
		case "--message", "-m":
			if i+1 >= len(args) {
				die(usageAliasSet)
			}
			j := i + 1
			for j < len(args) && args[j] != "--expect" && args[j] != "-e" && args[j] != "--message" && args[j] != "-m" {
				j++
			}
			if j == i+1 {
				die(usageAliasSet)
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
		die(usageAliasHistory)
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
		die(usageAliasGrant)
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
		die(usageAliasRevoke)
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
	if len(args) == 0 || (len(args) == 1 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help")) {
		aliasUsage()
		if len(args) == 0 {
			os.Exit(1)
		}
		os.Exit(0)
	}
	switch args[0] {
	case "ns", "namespace", "namespaces":
		if len(args) < 2 {
			aliasUsage()
			os.Exit(1)
		}
		switch args[1] {
		case "list":
			if len(args) != 2 {
				die(usageAliasNSList)
			}
			aliasNamespaceList(keyID)
		case "create":
			aliasNamespaceCreate(keyID, args[2:])
		default:
			aliasUsage()
			os.Exit(1)
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
		aliasUsage()
		os.Exit(1)
	}
}
