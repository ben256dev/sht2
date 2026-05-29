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
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"lukechampine.com/blake3"
	_ "modernc.org/sqlite"
)

var (
	blobDir   = getenv("SHT_BLOB_DIR", "/var/lib/sht/blobs")
	tmpDir    = getenv("SHT_TMP_DIR", "/var/lib/sht/tmp")
	sockDir   = getenv("SHT_SOCK_DIR", "/run/sht/sht.sock")
	sockMode  = getenv("SHT_SOCK_MODE", "0660")
	sockGroup = os.Getenv("SHT_SOCK_GROUP")
	dbPath    = getenv("SHT_DB_PATH", "/var/lib/sht/sht.db")
)

const defaultUserMaxBytes int64 = 30 * 1024 * 1024 * 1024

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func blobPath(digest string) string {
	if len(digest) < 3 {
		return filepath.Join(blobDir, digest)
	}
	return filepath.Join(blobDir, digest[:2], digest[2:])
}

func parseSocketMode(value string) (os.FileMode, error) {
	mode, err := strconv.ParseUint(value, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid SHT_SOCK_MODE %q: %w", value, err)
	}
	if mode > 0777 {
		return 0, fmt.Errorf("invalid SHT_SOCK_MODE %q: must be an octal permission mode no wider than 0777", value)
	}
	return os.FileMode(mode), nil
}

func lookupSocketGroupID(value string) (int, error) {
	if value == "" {
		return -1, nil
	}

	if gid, err := strconv.Atoi(value); err == nil {
		if gid < 0 {
			return -1, fmt.Errorf("invalid SHT_SOCK_GROUP %q: gid must be non-negative", value)
		}
		return gid, nil
	}

	group, err := user.LookupGroup(value)
	if err != nil {
		return -1, fmt.Errorf("invalid SHT_SOCK_GROUP %q: %w", value, err)
	}

	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return -1, fmt.Errorf("invalid SHT_SOCK_GROUP %q: resolved gid %q is not numeric", value, group.Gid)
	}
	return gid, nil
}

func ensureColumn(db *sql.DB, table, column, definition string) error {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition))
	return err
}

func userMaxBytes(db *sql.DB, userID int64) (int64, error) {
	var maxBytes int64
	err := db.QueryRow("SELECT max_bytes FROM users WHERE id = ?", userID).Scan(&maxBytes)
	if err != nil {
		return 0, err
	}
	return maxBytes, nil
}

func userStorageBytes(db *sql.DB, userID int64) (int64, error) {
	var size int64
	err := db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM blob_refs WHERE user_id = ?", userID).Scan(&size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func userHasBlob(db *sql.DB, userID int64, digest string) (bool, error) {
	var exists int
	err := db.QueryRow("SELECT 1 FROM blob_refs WHERE user_id = ? AND digest = ?", userID, digest).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func addBlobRef(db *sql.DB, p Principal, digest string, size int64) error {
	_, err := db.Exec(
		"INSERT INTO blob_refs (user_id, key_id, digest, size) VALUES (?, ?, ?, ?)",
		p.UserID,
		p.KeyID,
		digest,
		size,
	)
	return err
}

func checkUserQuota(db *sql.DB, userID, uploadBytes int64) error {
	maxBytes, err := userMaxBytes(db, userID)
	if err != nil {
		return err
	}

	usedBytes, err := userStorageBytes(db, userID)
	if err != nil {
		return err
	}

	if usedBytes+uploadBytes > maxBytes {
		return fmt.Errorf("quota exceeded: used %d bytes, upload %d bytes, limit %d bytes", usedBytes, uploadBytes, maxBytes)
	}
	return nil
}

type StoreBlobResponse struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Exists bool   `json:"exists"`
}

func handleBlobPost(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		digest := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
		final := blobPath(digest)

		if err := os.MkdirAll(filepath.Dir(final), 0755); err != nil {
			http.Error(w, "mkdir failed", http.StatusInternalServerError)
			return
		}

		exists, err := userHasBlob(db, p.UserID, digest)
		if err != nil {
			http.Error(w, "failed to check blob ref", http.StatusInternalServerError)
			return
		}
		if exists {
			ok = true
			os.Remove(tmpName)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(StoreBlobResponse{
				Digest: digest,
				Size:   n,
				Exists: true,
			})
			return
		}

		if err := checkUserQuota(db, p.UserID, n); err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}

		if _, err := os.Stat(final); err == nil {
			os.Remove(tmpName)
		} else if os.IsNotExist(err) {
			if err := os.Rename(tmpName, final); err != nil {
				http.Error(w, "rename failed", http.StatusInternalServerError)
				return
			}
		} else {
			http.Error(w, "failed to stat final blob", http.StatusInternalServerError)
			return
		}

		if err := addBlobRef(db, p, digest, n); err != nil {
			http.Error(w, "failed to add blob ref", http.StatusInternalServerError)
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
}

func handleBlobGet(db *sql.DB, w http.ResponseWriter, r *http.Request) {
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

	exists, err := userHasBlob(db, p.UserID, digest)
	if err != nil {
		http.Error(w, "failed to check blob ref", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "blob not found", http.StatusNotFound)
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
}

func openDB(path string) *sql.DB {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	enabled INTEGER NOT NULL DEFAULT 1,
	max_bytes INTEGER NOT NULL DEFAULT %d,
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
CREATE TABLE IF NOT EXISTS blob_refs (
	user_id INTEGER NOT NULL,
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (user_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);

CREATE INDEX IF NOT EXISTS blob_refs_user_id ON blob_refs(user_id);
CREATE INDEX IF NOT EXISTS blob_refs_digest ON blob_refs(digest);
`, defaultUserMaxBytes))
	if err != nil {
		log.Fatal(err)
	}

	if err := ensureColumn(db, "users", "max_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultUserMaxBytes)); err != nil {
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
	mode, err := parseSocketMode(sockMode)
	if err != nil {
		log.Fatal(err)
	}

	gid, err := lookupSocketGroupID(sockGroup)
	if err != nil {
		log.Fatal(err)
	}

	os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	if gid >= 0 {
		if err := os.Chown(path, -1, gid); err != nil {
			log.Fatal(err)
		}
	}

	if err := os.Chmod(path, mode); err != nil {
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

	mux.HandleFunc("/blob", requirePrincipal(db, handleBlobPost(db)))

	mux.HandleFunc("/blob/", requirePrincipal(db, func(w http.ResponseWriter, r *http.Request) {
		handleBlobGet(db, w, r)
	}))

	log.Println("Listening on ", path)
	log.Fatal(http.Serve(ln, mux))
}
