package main

import (
    "encoding/base64"
    "encoding/json"
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
	sockDir  = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func blobPath(digest string) string {
    if (len(digest) < 3) {
        return filepath.Join(blobDir, digest)
    }
    return filepath.Join(blobDir, digest[:2], digest[2:])
}

type StoreBlobResponse struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Exists bool   `json:"exists"`
}

func main() {
    path := sockDir

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

    mux.HandleFunc("/blob", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }

        defer r.Body.Close()

        tmp, err := os.CreateTemp(tmpDir, "blob-*")
        if err != nil {
            http.Error(w, "temp failed", http.StatusInternalServerError)
            return
        }

        tmpName := tmp.Name()
        closed := false
        ok := false

        defer func() {
            if !closed {
                tmp.Close()
            }
            if !ok {
                os.Remove(tmpName)
            }
        }()

        h := blake3.New(32, nil)

        n, err := io.Copy(io.MultiWriter(tmp, h), r.Body)
        if err != nil {
            http.Error(w, "upload failed", http.StatusBadRequest)
            return
        }

        if err := tmp.Close(); err != nil {
            http.Error(w, "close failed", http.StatusInternalServerError)
            return
        }
        closed = true

        digest := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
        final := blobPath(digest)

        if err := os.MkdirAll(filepath.Dir(final), 0755); err != nil {
            http.Error(w, "mkdir failed", http.StatusInternalServerError)
            return
        }

        if _, err := os.Stat(final); err == nil {
            ok = true
            os.Remove(tmpName)

            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(StoreBlobResponse{
                Digest: digest,
                Size:   n,
                Exists: true,
            })
            return
        } else if !os.IsNotExist(err) {
            http.Error(w, "failed to stat final blob", http.StatusInternalServerError)
            return
        }

        if err := os.Rename(tmpName, final); err != nil {
            http.Error(w, "rename failed", http.StatusInternalServerError)
            return
        }

        ok = true

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(StoreBlobResponse{
            Digest: digest,
            Size:   n,
            Exists: false,
        })
    })

    mux.HandleFunc("/blob/", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet && r.Method != http.MethodHead {
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
