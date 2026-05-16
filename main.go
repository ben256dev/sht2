package main

import (
    "encoding/base64"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
    "path/filepath"
    "strings"

    "lukechampine.com/blake3"
)

var (
	blobDir = getenv("SHT_BLOB_DIR", "/var/lib/sht/blobs")
	tmpDir  = getenv("SHT_TMP_DIR", "/var/lib/sht/tmp")
)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func blobPath(digest string) string {
	return filepath.Join(blobDir, digest)
}

func main() {
    path := "/run/sht/sht.sock"

    os.Remove(path)

    ln, err := net.Listen("unix", path)
    if err != nil {
        log.Fatal(err)
    }
    defer ln.Close()

    if err := os.Chmod(path, 0666); err != nil {
        log.Fatal(err)
    }

    if err := os.MkdirAll(blobDir, 0755); err != nil {
        log.Fatal(err)
    }

    if err := os.MkdirAll(tmpDir, 0700); err != nil {
        log.Fatal(err)
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintln(w, "pong")
    })

    mux.HandleFunc("/blob", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }

        defer r.Body.Close()

        h := blake3.New(32, nil)

        tmp, err := os.CreateTemp(tmpDir, "blob-*")
        if (err != nil) {
            http.Error(w, "failed to create temp file", http.StatusInternalServerError)
            return
        }
        defer os.Remove(tmp.Name())

        n, err := io.Copy(io.MultiWriter(tmp, h), r.Body)
        if err != nil {
            http.Error(w, "upload failed", http.StatusInternalServerError)
            return
        }

        dgst := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

        if err := tmp.Close(); err != nil {
            http.Error(w, "failed to close temp file", http.StatusInternalServerError)
            return
        }

        final := blobPath(dgst)

        if _, err := os.Stat(final); err == nil {
            fmt.Fprintf(w, `{"digest":"%s","size":%d,"exists":true}`, dgst, n)
            return
        }

        if err := os.Rename(tmp.Name(), final); err != nil {
            http.Error(w, "failed to store blob", http.StatusInternalServerError)
            return
        }

        fmt.Fprintln(w, `{"digest":"%s","size":%d}`, dgst, n)
    })

    mux.HandleFunc("/blob/", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }

        digest := strings.TrimPrefix(r.URL.Path, "/blob/")
        if digest == "" || strings.Contains(digest, "/") || strings.Contains(digest, "..") {
            http.Error(w, "invalid digest", http.StatusBadRequest)
            return
        }

        path := blobPath(digest)

        f, err := os.Open(path)
        if os.IsNotExist(err) {
            http.Error(w, "blob not found", http.StatusNotFound)
            return
        }
        if err != nil {
            http.Error(w, "failed to open blob", http.StatusInternalServerError)
            return
        }
        defer f.Close()

        info, err := f.Stat()
        if err != nil {
            http.Error(w, "failed to stat blob", http.StatusInternalServerError)
            return
        }

        http.ServeContent(w, r, digest, info.ModTime(), f)
    })

    log.Println("Listening on ", path)
    log.Fatal(http.Serve(ln, mux))
}
