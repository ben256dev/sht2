package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"lukechampine.com/blake3"
	_ "modernc.org/sqlite"
)

var (
	blobDir = getenv("SHT_BLOB_DIR", "/var/lib/sht/blobs")
	tmpDir  = getenv("SHT_TMP_DIR", "/var/lib/sht/tmp")
	sockDir = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
	dbPath  = getenv("SHT_DB_PATH", "/var/lib/sht/sht.db")
)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func blobPath(keyID int64, digest string) string {
	keyDir := filepath.Join(blobDir, strconv.FormatInt(keyID, 10))
	if len(digest) < 3 {
		return filepath.Join(keyDir, digest)
	}
	return filepath.Join(keyDir, digest[:2], digest[2:])
}

type StoreBlobResponse struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Exists bool   `json:"exists"`
}

func handleBlobPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	p := getPrincipal(r)
	fmt.Println("user:", p.UserName)

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

	keyID := p.KeyID
	digest := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
	final := blobPath(keyID, digest)

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
}

func handleBlobGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p := getPrincipal(r)
	fmt.Println("user:", p.UserName)

	digest := strings.TrimPrefix(r.URL.Path, "/blob/")
	if digest == "" || strings.Contains(digest, "/") || strings.Contains(digest, "..") {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	keyID := p.KeyID
	path := blobPath(keyID, digest)

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
}

func openDB(path string) *sql.DB {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS key_ids (
	id INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY(user_id) REFERENCES users(id)
);
`)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

type ctxKey string

type Principal struct {
	UserID   int64
	UserName string
	KeyID    int64
	KeyName  string
}

const principalCtxKey ctxKey = "principal"

func getPrincipal(r *http.Request) Principal {
	return r.Context().Value(principalCtxKey).(Principal)
}

func principalFromKeyID(db *sql.DB, keyID int64) (Principal, error) {
	var p Principal

	err := db.QueryRow(`
		SELECT
			users.id,
			users.name,
			key_ids.id,
			key_ids.name
		FROM key_ids
		JOIN users ON users.id = key_ids.user_id
		WHERE key_ids.id = ?
		  AND key_ids.enabled = 1
		  AND users.enabled = 1
	`, keyID).Scan(&p.UserID, &p.UserName, &p.KeyID, &p.KeyName)

	if err != nil {
		return Principal{}, err
	}

	return p, nil
}

func requirePrincipal(db *sql.DB, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		keyIDText := r.Header.Get("X-SHT-Key-ID")
		if keyIDText == "" {
			http.Error(w, "missing key id", http.StatusUnauthorized)
			return
		}

		keyID, err := strconv.ParseInt(keyIDText, 10, 64)
		if err != nil || keyID <= 0 {
			http.Error(w, "invalid key id", http.StatusUnauthorized)
			return
		}

		p, err := principalFromKeyID(db, keyID)
		if err == sql.ErrNoRows {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if err != nil {
			http.Error(w, "auth failed", http.StatusInternalServerError)
			return
		}

		ctx := context.WithValue(r.Context(), principalCtxKey, p)
		next(w, r.WithContext(ctx))
	}
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

	db := openDB(dbPath)
	defer db.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/blob", requirePrincipal(db, handleBlobPost))

	mux.HandleFunc("/blob/", requirePrincipal(db, handleBlobGet))

	log.Println("Listening on ", path)
	log.Fatal(http.Serve(ln, mux))
}
