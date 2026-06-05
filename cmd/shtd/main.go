package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
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

const defaultUserMaxBytes int64 = 3 * 1024 * 1024 * 1024
const defaultUserMaxPendingBytes int64 = defaultUserMaxBytes + defaultUserMaxBytes/4
const defaultMaxSimpleUploadBytes int64 = 64 * 1024 * 1024
const defaultChunkSize int64 = 8 * 1024 * 1024
const defaultShelfName = "main"

var errSimpleUploadTooLarge = errors.New("simple upload too large; use manifest/resumable upload")
var shelfNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

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

func chunkPath(digest string) string {
	if len(digest) < 3 {
		return filepath.Join(blobDir, "chunks", digest)
	}
	return filepath.Join(blobDir, "chunks", digest[:2], digest[2:])
}

func encodeDigest(sum []byte) string {
	return base64.RawURLEncoding.EncodeToString(sum)
}

func validDigest(digest string) bool {
	sum, err := base64.RawURLEncoding.DecodeString(digest)
	return err == nil && len(sum) == 32
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

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func userMaxBytes(db *sql.DB, userID int64) (int64, error) {
	var maxBytes int64
	err := db.QueryRow("SELECT max_bytes FROM users WHERE id = ?", userID).Scan(&maxBytes)
	if err != nil {
		return 0, err
	}
	return maxBytes, nil
}

func userMaxSimpleUploadBytes(db *sql.DB, userID int64) (int64, error) {
	var maxBytes int64
	err := db.QueryRow("SELECT max_simple_upload_bytes FROM users WHERE id = ?", userID).Scan(&maxBytes)
	if err != nil {
		return 0, err
	}
	return maxBytes, nil
}

func userMultiShelfEnabled(db *sql.DB, userID int64) (bool, error) {
	var enabled int
	err := db.QueryRow("SELECT multi_shelf_enabled FROM users WHERE id = ?", userID).Scan(&enabled)
	if err != nil {
		return false, err
	}
	return enabled != 0, nil
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

type ShelfCreateRequest struct {
	Name            string `json:"name"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
}

type ShelfRenameRequest struct {
	Name string `json:"name"`
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
	ID                int64   `json:"id"`
	Namespace         string  `json:"namespace"`
	Path              string  `json:"path"`
	Digest            string  `json:"digest"`
	AuthorUserID      int64   `json:"author_user_id"`
	AuthorUser        string  `json:"author_user"`
	PreviousVersionID *int64  `json:"previous_version_id,omitempty"`
	Message           *string `json:"message,omitempty"`
	CreatedAt         string  `json:"created_at"`
}

type AliasVersionListResponse struct {
	Versions []AliasVersion `json:"versions"`
}

type AliasVersionCreateRequest struct {
	Digest          string `json:"digest"`
	ExpectedVersion *int64 `json:"expected_version"`
	Message         string `json:"message,omitempty"`
}

type AliasConflictResponse struct {
	Error          string `json:"error"`
	CurrentVersion int64  `json:"current_version"`
}

func shelfNameReserved(name string) bool {
	switch name {
	case "--", "cat", "stat", "release", "list", "refs", "quota", "manifest", "upload", "upload-chunk", "status", "upload-status", "finalize", "help", "shelf", "alias":
		return true
	default:
		return false
	}
}

func validateAliasNamespaceName(name string) error {
	if err := validateShelfName(name); err != nil {
		return err
	}
	if name == "namespaces" {
		return fmt.Errorf("reserved alias namespace")
	}
	return nil
}

func normalizeAliasPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "/")
	if path == "" {
		return "", fmt.Errorf("invalid alias path")
	}
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if !shelfNamePattern.MatchString(part) || part == "." || part == ".." {
			return "", fmt.Errorf("invalid alias path")
		}
	}
	switch parts[0] {
	case "grants":
		return "", fmt.Errorf("reserved alias path")
	}
	switch parts[len(parts)-1] {
	case "blob", "versions":
		return "", fmt.Errorf("reserved alias path")
	}
	return strings.Join(parts, "/"), nil
}

func normalizeAliasGrantPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return "", nil
	}
	return normalizeAliasPath(path)
}

func roleRank(role string) int {
	switch role {
	case "read":
		return 1
	case "write":
		return 2
	case "admin":
		return 3
	default:
		return 0
	}
}

func validateAliasRole(role string) error {
	if roleRank(role) == 0 {
		return fmt.Errorf("invalid alias role")
	}
	return nil
}

func validateShelfName(name string) error {
	if !shelfNamePattern.MatchString(name) {
		return fmt.Errorf("invalid shelf name")
	}
	if shelfNameReserved(name) {
		return fmt.Errorf("reserved shelf name")
	}
	return nil
}

func validateNewShelfName(name string) error {
	return validateShelfName(name)
}

func defaultShelfID(db *sql.DB, userID int64) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT id FROM shelves WHERE user_id = ? AND is_default = 1 AND enabled = 1", userID).Scan(&id)
	return id, err
}

func shelfByName(db *sql.DB, userID int64, name string) (Shelf, error) {
	var s Shelf
	var enabled, isDefault int
	err := db.QueryRow(`
		SELECT id, name, max_bytes, max_pending_bytes, pending_bytes, enabled, is_default
		FROM shelves
		WHERE user_id = ? AND name = ?
	`, userID, name).Scan(&s.ID, &s.Name, &s.MaxBytes, &s.MaxPendingBytes, &s.PendingBytes, &enabled, &isDefault)
	s.Enabled = enabled != 0
	s.IsDefault = isDefault != 0
	return s, err
}

func resolveShelf(db *sql.DB, p Principal, name string, required bool) (Shelf, error) {
	multi, err := userMultiShelfEnabled(db, p.UserID)
	if err != nil {
		return Shelf{}, err
	}
	if name == "" {
		if multi && required {
			return Shelf{}, fmt.Errorf("shelf required: run `sht shelf list` to choose a shelf, then retry as `sht <shelf> <command>`; direct socket clients should choose from GET /shelves and send X-SHT-Shelf")
		}
		id, err := defaultShelfID(db, p.UserID)
		if err != nil {
			return Shelf{}, err
		}
		return Shelf{ID: id, Enabled: true, IsDefault: true}, nil
	}
	if err := validateShelfName(name); err != nil {
		return Shelf{}, err
	}
	s, err := shelfByName(db, p.UserID, name)
	if err == sql.ErrNoRows {
		return Shelf{}, fmt.Errorf("shelf not found: run `sht shelf list` to choose an existing shelf, or create it with `sht shelf create %s <max_bytes> <max_pending_bytes>`", name)
	}
	if err != nil {
		return Shelf{}, err
	}
	if !s.Enabled {
		return Shelf{}, fmt.Errorf("shelf disabled")
	}
	return s, nil
}

func requestShelfName(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-SHT-Shelf"))
}

func userStorageBytes(db *sql.DB, userID int64) (int64, error) {
	var size int64
	err := db.QueryRow(`
		SELECT COALESCE(SUM(size), 0)
		FROM (
			SELECT digest, MAX(size) AS size
			FROM blob_refs
			WHERE user_id = ? AND dirty = 0
			GROUP BY digest
		)
	`, userID).Scan(&size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func userCleanDigestCount(db *sql.DB, userID int64) (int64, error) {
	var count int64
	err := db.QueryRow("SELECT COUNT(DISTINCT digest) FROM blob_refs WHERE user_id = ? AND dirty = 0", userID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func shelfStorageBytes(db *sql.DB, shelfID int64) (int64, error) {
	var size int64
	err := db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM blob_refs WHERE shelf_id = ? AND dirty = 0", shelfID).Scan(&size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func shelfRefCount(db *sql.DB, shelfID int64) (int64, error) {
	var count int64
	err := db.QueryRow("SELECT COUNT(*) FROM blob_refs WHERE shelf_id = ?", shelfID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func userUploadReservedBytes(db *sql.DB, userID int64, excludeDigest string) (int64, error) {
	var size int64
	var err error
	if excludeDigest == "" {
		err = db.QueryRow(`
			SELECT COALESCE(SUM(size), 0)
			FROM (
				SELECT digest, MAX(size) AS size
				FROM upload_sessions
				WHERE user_id = ?
				GROUP BY digest
			)
		`, userID).Scan(&size)
	} else {
		err = db.QueryRow(`
			SELECT COALESCE(SUM(size), 0)
			FROM (
				SELECT digest, MAX(size) AS size
				FROM upload_sessions
				WHERE user_id = ? AND digest != ?
				GROUP BY digest
			)
		`, userID, excludeDigest).Scan(&size)
	}
	if err != nil {
		return 0, err
	}
	return size, nil
}

func shelfUploadReservedBytes(db *sql.DB, shelfID int64, excludeDigest string) (int64, error) {
	var size int64
	var err error
	if excludeDigest == "" {
		err = db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM upload_sessions WHERE shelf_id = ?", shelfID).Scan(&size)
	} else {
		err = db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM upload_sessions WHERE shelf_id = ? AND digest != ?", shelfID, excludeDigest).Scan(&size)
	}
	if err != nil {
		return 0, err
	}
	return size, nil
}

func userPendingBytes(db *sql.DB, userID int64) (int64, int64, error) {
	var pendingBytes, maxPendingBytes int64
	err := db.QueryRow("SELECT pending_bytes, max_pending_bytes FROM users WHERE id = ?", userID).Scan(&pendingBytes, &maxPendingBytes)
	if err != nil {
		return 0, 0, err
	}
	return pendingBytes, maxPendingBytes, nil
}

func shelfPendingBytes(db *sql.DB, shelfID int64) (int64, int64, error) {
	var pendingBytes, maxPendingBytes int64
	err := db.QueryRow("SELECT pending_bytes, max_pending_bytes FROM shelves WHERE id = ?", shelfID).Scan(&pendingBytes, &maxPendingBytes)
	if err != nil {
		return 0, 0, err
	}
	return pendingBytes, maxPendingBytes, nil
}

func userBlobRefState(db *sql.DB, userID, shelfID int64, digest string) (bool, bool, error) {
	var dirty int
	err := db.QueryRow("SELECT dirty FROM blob_refs WHERE user_id = ? AND shelf_id = ? AND digest = ?", userID, shelfID, digest).Scan(&dirty)
	if err == sql.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, dirty != 0, nil
}

func userHasCleanBlob(db *sql.DB, userID int64, shelfID int64, digest string) (bool, error) {
	var exists int
	var err error
	if shelfID > 0 {
		err = db.QueryRow("SELECT 1 FROM blob_refs WHERE user_id = ? AND shelf_id = ? AND digest = ? AND dirty = 0", userID, shelfID, digest).Scan(&exists)
	} else {
		err = db.QueryRow("SELECT 1 FROM blob_refs WHERE user_id = ? AND digest = ? AND dirty = 0", userID, digest).Scan(&exists)
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func addBlobRef(db *sql.DB, p Principal, shelfID int64, digest string, size int64) error {
	_, err := db.Exec(
		"INSERT INTO blob_refs (user_id, shelf_id, key_id, digest, size) VALUES (?, ?, ?, ?, ?)",
		p.UserID,
		shelfID,
		p.KeyID,
		digest,
		size,
	)
	return err
}

func restoreBlobRef(db *sql.DB, p Principal, shelfID int64, digest string, size int64) error {
	_, err := db.Exec(
		"UPDATE blob_refs SET key_id = ?, size = ?, dirty = 0, created_at = CURRENT_TIMESTAMP WHERE user_id = ? AND shelf_id = ? AND digest = ?",
		p.KeyID,
		size,
		p.UserID,
		shelfID,
		digest,
	)
	return err
}

func releaseBlobRef(db *sql.DB, userID, shelfID int64, digest string) (bool, error) {
	result, err := db.Exec("UPDATE blob_refs SET dirty = 1 WHERE user_id = ? AND shelf_id = ? AND digest = ? AND dirty = 0", userID, shelfID, digest)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func checkUserQuota(db *sql.DB, userID, uploadBytes int64) error {
	return checkUserQuotaExcludingUpload(db, userID, uploadBytes, "")
}

func checkUserQuotaExcludingUpload(db *sql.DB, userID, uploadBytes int64, excludeDigest string) error {
	maxBytes, err := userMaxBytes(db, userID)
	if err != nil {
		return err
	}

	usedBytes, err := userStorageBytes(db, userID)
	if err != nil {
		return err
	}

	reservedBytes, err := userUploadReservedBytes(db, userID, excludeDigest)
	if err != nil {
		return err
	}

	if usedBytes+reservedBytes+uploadBytes > maxBytes {
		return fmt.Errorf("quota exceeded: used %d bytes, reserved %d bytes, upload %d bytes, limit %d bytes", usedBytes, reservedBytes, uploadBytes, maxBytes)
	}
	return nil
}

func userQuotaUploadBytes(db *sql.DB, userID int64, digest string, size int64) (int64, error) {
	exists, err := userHasCleanBlob(db, userID, 0, digest)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, nil
	}
	return size, nil
}

func checkUserPendingQuota(db *sql.DB, userID, uploadBytes int64) error {
	return checkUserPendingQuotaExcludingUpload(db, userID, uploadBytes, "")
}

func checkUserPendingQuotaExcludingUpload(db *sql.DB, userID, uploadBytes int64, excludeDigest string) error {
	pendingBytes, maxPendingBytes, err := userPendingBytes(db, userID)
	if err != nil {
		return err
	}

	reservedBytes, err := userUploadReservedBytes(db, userID, excludeDigest)
	if err != nil {
		return err
	}

	if pendingBytes+reservedBytes+uploadBytes > maxPendingBytes {
		return fmt.Errorf("pending quota exceeded: pending %d bytes, reserved %d bytes, upload %d bytes, limit %d bytes", pendingBytes, reservedBytes, uploadBytes, maxPendingBytes)
	}
	return nil
}

func checkShelfQuotaExcludingUpload(db *sql.DB, shelfID, uploadBytes int64, excludeDigest string) error {
	var maxBytes int64
	err := db.QueryRow("SELECT max_bytes FROM shelves WHERE id = ?", shelfID).Scan(&maxBytes)
	if err != nil {
		return err
	}

	usedBytes, err := shelfStorageBytes(db, shelfID)
	if err != nil {
		return err
	}
	reservedBytes, err := shelfUploadReservedBytes(db, shelfID, excludeDigest)
	if err != nil {
		return err
	}
	if usedBytes+reservedBytes+uploadBytes > maxBytes {
		return fmt.Errorf("shelf quota exceeded: used %d bytes, reserved %d bytes, upload %d bytes, limit %d bytes", usedBytes, reservedBytes, uploadBytes, maxBytes)
	}
	return nil
}

func checkShelfPendingQuotaExcludingUpload(db *sql.DB, shelfID, uploadBytes int64, excludeDigest string) error {
	pendingBytes, maxPendingBytes, err := shelfPendingBytes(db, shelfID)
	if err != nil {
		return err
	}
	reservedBytes, err := shelfUploadReservedBytes(db, shelfID, excludeDigest)
	if err != nil {
		return err
	}
	if pendingBytes+reservedBytes+uploadBytes > maxPendingBytes {
		return fmt.Errorf("shelf pending quota exceeded: pending %d bytes, reserved %d bytes, upload %d bytes, limit %d bytes", pendingBytes, reservedBytes, uploadBytes, maxPendingBytes)
	}
	return nil
}

func addPendingBytes(db *sql.DB, userID, uploadBytes int64) error {
	_, err := db.Exec("UPDATE users SET pending_bytes = pending_bytes + ? WHERE id = ?", uploadBytes, userID)
	return err
}

func addShelfPendingBytes(db *sql.DB, shelfID, uploadBytes int64) error {
	_, err := db.Exec("UPDATE shelves SET pending_bytes = pending_bytes + ? WHERE id = ?", uploadBytes, shelfID)
	return err
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

type ManifestChunk struct {
	Index  int64  `json:"index"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type ManifestRequest struct {
	Digest    string          `json:"digest"`
	Size      int64           `json:"size"`
	ChunkSize int64           `json:"chunk_size"`
	Chunks    []ManifestChunk `json:"chunks"`
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

func userBlobRefs(db *sql.DB, userID, shelfID int64) ([]BlobRef, error) {
	query := `
		SELECT blob_refs.digest, blob_refs.size, blob_refs.key_id, shelves.name, blob_refs.created_at, blob_refs.dirty
		FROM blob_refs
		JOIN shelves ON shelves.id = blob_refs.shelf_id
		WHERE blob_refs.user_id = ?
	`
	args := []any{userID}
	if shelfID > 0 {
		query += " AND blob_refs.shelf_id = ?"
		args = append(args, shelfID)
	}
	query += `
		ORDER BY blob_refs.created_at DESC, blob_refs.digest ASC
	`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []BlobRef
	for rows.Next() {
		var ref BlobRef
		var dirty int
		if err := rows.Scan(&ref.Digest, &ref.Size, &ref.KeyID, &ref.Shelf, &ref.CreatedAt, &dirty); err != nil {
			return nil, err
		}
		ref.Dirty = dirty != 0
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func normalizeManifest(m ManifestRequest) (ManifestRequest, error) {
	if !validDigest(m.Digest) {
		return ManifestRequest{}, fmt.Errorf("invalid digest")
	}
	if m.Size < 0 {
		return ManifestRequest{}, fmt.Errorf("invalid size")
	}
	if m.ChunkSize == 0 {
		m.ChunkSize = defaultChunkSize
	}
	if m.ChunkSize != defaultChunkSize {
		return ManifestRequest{}, fmt.Errorf("chunk_size must be %d", defaultChunkSize)
	}
	if m.Size == 0 {
		if len(m.Chunks) != 0 {
			return ManifestRequest{}, fmt.Errorf("empty blob must not have chunks")
		}
		return m, nil
	}
	if len(m.Chunks) == 0 {
		return ManifestRequest{}, fmt.Errorf("manifest must include chunks")
	}

	var total int64
	for i, chunk := range m.Chunks {
		if chunk.Index != int64(i) {
			return ManifestRequest{}, fmt.Errorf("chunk indexes must be contiguous")
		}
		if !validDigest(chunk.Digest) {
			return ManifestRequest{}, fmt.Errorf("invalid chunk digest at index %d", i)
		}
		if chunk.Size <= 0 || chunk.Size > m.ChunkSize {
			return ManifestRequest{}, fmt.Errorf("invalid chunk size at index %d", i)
		}
		if i < len(m.Chunks)-1 && chunk.Size != m.ChunkSize {
			return ManifestRequest{}, fmt.Errorf("non-final chunk size must be %d", m.ChunkSize)
		}
		total += chunk.Size
	}
	if total != m.Size {
		return ManifestRequest{}, fmt.Errorf("manifest size mismatch")
	}
	return m, nil
}

func manifestExists(db *sql.DB, digest string) (bool, error) {
	var exists int
	err := db.QueryRow("SELECT 1 FROM blob_manifests WHERE digest = ?", digest).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func chunkExists(digest string) (bool, error) {
	_, err := os.Stat(chunkPath(digest))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func writeChunk(digest string, data []byte) (bool, error) {
	path := chunkPath(digest)
	if exists, err := chunkExists(digest); err != nil {
		return false, err
	} else if exists {
		return true, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, err
	}

	tmp, err := os.CreateTemp(tmpDir, "chunk-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		tmp.Close()
		if !ok {
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			ok = true
			return true, nil
		}
		return false, err
	}
	ok = true
	return false, nil
}

func insertManifest(db *sql.DB, m ManifestRequest) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO blob_manifests (digest, size, chunk_size) VALUES (?, ?, ?)",
		m.Digest,
		m.Size,
		m.ChunkSize,
	); err != nil {
		return err
	}
	for _, chunk := range m.Chunks {
		if _, err := tx.Exec(
			"INSERT OR IGNORE INTO blob_manifest_chunks (digest, chunk_index, chunk_digest, size) VALUES (?, ?, ?, ?)",
			m.Digest,
			chunk.Index,
			chunk.Digest,
			chunk.Size,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func readSimpleManifest(r io.Reader, limit int64) (ManifestRequest, [][]byte, error) {
	if limit <= 0 {
		return ManifestRequest{}, nil, errSimpleUploadTooLarge
	}

	h := blake3.New(32, nil)
	chunkSize := int(defaultChunkSize)
	buf := make([]byte, chunkSize)
	var chunks []ManifestChunk
	var payloads [][]byte
	var size int64

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			size += int64(n)
			if size > limit {
				return ManifestRequest{}, nil, errSimpleUploadTooLarge
			}
			data := append([]byte(nil), buf[:n]...)
			h.Write(data)
			ch := blake3.Sum256(data)
			chunks = append(chunks, ManifestChunk{
				Index:  int64(len(chunks)),
				Digest: encodeDigest(ch[:]),
				Size:   int64(n),
			})
			payloads = append(payloads, data)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return ManifestRequest{}, nil, err
		}
	}

	return ManifestRequest{
		Digest:    encodeDigest(h.Sum(nil)),
		Size:      size,
		ChunkSize: defaultChunkSize,
		Chunks:    chunks,
	}, payloads, nil
}

func createOrRestoreRef(db *sql.DB, p Principal, shelfID int64, digest string, size int64) (bool, error) {
	refExists, refDirty, err := userBlobRefState(db, p.UserID, shelfID, digest)
	if err != nil {
		return false, err
	}
	if refExists && !refDirty {
		return true, nil
	}
	if refExists {
		if err := restoreBlobRef(db, p, shelfID, digest, size); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := addBlobRef(db, p, shelfID, digest, size); err != nil {
		return false, err
	}
	return false, nil
}

func storeSimpleManifest(db *sql.DB, p Principal, shelfID int64, m ManifestRequest, payloads [][]byte) (bool, error) {
	refExists, refDirty, err := userBlobRefState(db, p.UserID, shelfID, m.Digest)
	if err != nil {
		return false, err
	}
	if refExists && !refDirty {
		return true, nil
	}

	userUploadBytes, err := userQuotaUploadBytes(db, p.UserID, m.Digest, m.Size)
	if err != nil {
		return false, err
	}
	if err := checkUserQuota(db, p.UserID, userUploadBytes); err != nil {
		return false, err
	}
	if err := checkShelfQuotaExcludingUpload(db, shelfID, m.Size, ""); err != nil {
		return false, err
	}

	exists, err := manifestExists(db, m.Digest)
	if err != nil {
		return false, err
	}

	var physicalBytes int64
	if !exists {
		for _, chunk := range m.Chunks {
			chunkAlreadyExisted, err := chunkExists(chunk.Digest)
			if err != nil {
				return false, err
			}
			if !chunkAlreadyExisted {
				physicalBytes += chunk.Size
			}
		}
		if err := checkUserPendingQuota(db, p.UserID, physicalBytes); err != nil {
			return false, err
		}
		if err := checkShelfPendingQuotaExcludingUpload(db, shelfID, physicalBytes, ""); err != nil {
			return false, err
		}
		for i, data := range payloads {
			if _, err := writeChunk(m.Chunks[i].Digest, data); err != nil {
				return false, err
			}
		}
		if err := insertManifest(db, m); err != nil {
			return false, err
		}
		if physicalBytes > 0 {
			if err := addPendingBytes(db, p.UserID, physicalBytes); err != nil {
				return false, err
			}
			if err := addShelfPendingBytes(db, shelfID, physicalBytes); err != nil {
				return false, err
			}
		}
	}

	_, err = createOrRestoreRef(db, p, shelfID, m.Digest, m.Size)
	return false, err
}

func manifestForDigest(db *sql.DB, digest string) (ManifestRequest, error) {
	var m ManifestRequest
	err := db.QueryRow("SELECT digest, size, chunk_size FROM blob_manifests WHERE digest = ?", digest).Scan(&m.Digest, &m.Size, &m.ChunkSize)
	if err != nil {
		return ManifestRequest{}, err
	}

	rows, err := db.Query(`
		SELECT chunk_index, chunk_digest, size
		FROM blob_manifest_chunks
		WHERE digest = ?
		ORDER BY chunk_index ASC
	`, digest)
	if err != nil {
		return ManifestRequest{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var chunk ManifestChunk
		if err := rows.Scan(&chunk.Index, &chunk.Digest, &chunk.Size); err != nil {
			return ManifestRequest{}, err
		}
		m.Chunks = append(m.Chunks, chunk)
	}
	return m, rows.Err()
}

func copyManifestBlob(w io.Writer, m ManifestRequest) error {
	for _, chunk := range m.Chunks {
		f, err := os.Open(chunkPath(chunk.Digest))
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func uploadSessionManifest(db *sql.DB, userID, shelfID int64, digest string) (ManifestRequest, int64, error) {
	var m ManifestRequest
	var physicalPendingBytes int64
	err := db.QueryRow(
		"SELECT digest, size, chunk_size, physical_pending_bytes FROM upload_sessions WHERE user_id = ? AND shelf_id = ? AND digest = ?",
		userID,
		shelfID,
		digest,
	).Scan(&m.Digest, &m.Size, &m.ChunkSize, &physicalPendingBytes)
	if err != nil {
		return ManifestRequest{}, 0, err
	}

	rows, err := db.Query(`
		SELECT chunk_index, chunk_digest, size
		FROM upload_session_chunks
		WHERE user_id = ? AND shelf_id = ? AND digest = ?
		ORDER BY chunk_index ASC
	`, userID, shelfID, digest)
	if err != nil {
		return ManifestRequest{}, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var chunk ManifestChunk
		if err := rows.Scan(&chunk.Index, &chunk.Digest, &chunk.Size); err != nil {
			return ManifestRequest{}, 0, err
		}
		m.Chunks = append(m.Chunks, chunk)
	}
	return m, physicalPendingBytes, rows.Err()
}

func sameManifest(a, b ManifestRequest) bool {
	if a.Digest != b.Digest || a.Size != b.Size || a.ChunkSize != b.ChunkSize || len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i] != b.Chunks[i] {
			return false
		}
	}
	return true
}

func uploadStatus(db *sql.DB, userID, shelfID int64, digest string) (UploadStatusResponse, error) {
	manifest, physicalPendingBytes, err := uploadSessionManifest(db, userID, shelfID, digest)
	if err != nil {
		return UploadStatusResponse{}, err
	}

	uploadedRows, err := db.Query(`
		SELECT chunk_index
		FROM upload_session_chunks
		WHERE user_id = ? AND shelf_id = ? AND digest = ? AND uploaded = 1
		ORDER BY chunk_index ASC
	`, userID, shelfID, digest)
	if err != nil {
		return UploadStatusResponse{}, err
	}
	defer uploadedRows.Close()

	uploadedSet := make(map[int64]bool)
	uploaded := []int64{}
	for uploadedRows.Next() {
		var idx int64
		if err := uploadedRows.Scan(&idx); err != nil {
			return UploadStatusResponse{}, err
		}
		uploadedSet[idx] = true
		uploaded = append(uploaded, idx)
	}
	if err := uploadedRows.Err(); err != nil {
		return UploadStatusResponse{}, err
	}

	missing := []int64{}
	for _, chunk := range manifest.Chunks {
		if !uploadedSet[chunk.Index] {
			missing = append(missing, chunk.Index)
		}
	}

	return UploadStatusResponse{
		Digest:          manifest.Digest,
		Size:            manifest.Size,
		ChunkSize:       manifest.ChunkSize,
		Missing:         missing,
		Uploaded:        uploaded,
		Complete:        len(missing) == 0,
		Finalized:       false,
		PhysicalPending: physicalPendingBytes,
	}, nil
}

func createUploadSession(db *sql.DB, p Principal, shelfID int64, manifest ManifestRequest) (UploadStatusResponse, error) {
	if exists, err := manifestExists(db, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	} else if exists {
		userUploadBytes, err := userQuotaUploadBytes(db, p.UserID, manifest.Digest, manifest.Size)
		if err != nil {
			return UploadStatusResponse{}, err
		}
		if err := checkUserQuota(db, p.UserID, userUploadBytes); err != nil {
			return UploadStatusResponse{}, err
		}
		if err := checkShelfQuotaExcludingUpload(db, shelfID, manifest.Size, ""); err != nil {
			return UploadStatusResponse{}, err
		}
		_, err = createOrRestoreRef(db, p, shelfID, manifest.Digest, manifest.Size)
		if err != nil {
			return UploadStatusResponse{}, err
		}
		return UploadStatusResponse{
			Digest:    manifest.Digest,
			Size:      manifest.Size,
			ChunkSize: manifest.ChunkSize,
			Missing:   []int64{},
			Uploaded:  []int64{},
			Complete:  true,
			Finalized: true,
		}, nil
	}

	existing, _, err := uploadSessionManifest(db, p.UserID, shelfID, manifest.Digest)
	if err == nil {
		if !sameManifest(existing, manifest) {
			return UploadStatusResponse{}, fmt.Errorf("upload session already exists with different manifest")
		}
		return uploadStatus(db, p.UserID, shelfID, manifest.Digest)
	}
	if err != sql.ErrNoRows {
		return UploadStatusResponse{}, err
	}

	userUploadBytes, err := userQuotaUploadBytes(db, p.UserID, manifest.Digest, manifest.Size)
	if err != nil {
		return UploadStatusResponse{}, err
	}
	if err := checkUserQuotaExcludingUpload(db, p.UserID, userUploadBytes, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}
	if err := checkUserPendingQuotaExcludingUpload(db, p.UserID, manifest.Size, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}
	if err := checkShelfQuotaExcludingUpload(db, shelfID, manifest.Size, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}
	if err := checkShelfPendingQuotaExcludingUpload(db, shelfID, manifest.Size, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}

	tx, err := db.Begin()
	if err != nil {
		return UploadStatusResponse{}, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		"INSERT INTO upload_sessions (user_id, shelf_id, key_id, digest, size, chunk_size) VALUES (?, ?, ?, ?, ?, ?)",
		p.UserID,
		shelfID,
		p.KeyID,
		manifest.Digest,
		manifest.Size,
		manifest.ChunkSize,
	); err != nil {
		return UploadStatusResponse{}, err
	}
	for _, chunk := range manifest.Chunks {
		uploaded := 0
		if exists, err := chunkExists(chunk.Digest); err != nil {
			return UploadStatusResponse{}, err
		} else if exists {
			uploaded = 1
		}
		if _, err := tx.Exec(
			"INSERT INTO upload_session_chunks (user_id, shelf_id, digest, chunk_index, chunk_digest, size, uploaded) VALUES (?, ?, ?, ?, ?, ?, ?)",
			p.UserID,
			shelfID,
			manifest.Digest,
			chunk.Index,
			chunk.Digest,
			chunk.Size,
			uploaded,
		); err != nil {
			return UploadStatusResponse{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return UploadStatusResponse{}, err
	}

	return uploadStatus(db, p.UserID, shelfID, manifest.Digest)
}

func markUploadChunk(db *sql.DB, userID, shelfID int64, digest string, index int64, physicalBytes int64) error {
	_, err := db.Exec(`
		UPDATE upload_sessions
		SET physical_pending_bytes = physical_pending_bytes + ?, updated_at = CURRENT_TIMESTAMP
		WHERE user_id = ? AND shelf_id = ? AND digest = ?
	`, physicalBytes, userID, shelfID, digest)
	if err != nil {
		return err
	}
	result, err := db.Exec(`
		UPDATE upload_session_chunks
		SET uploaded = 1
		WHERE user_id = ? AND shelf_id = ? AND digest = ? AND chunk_index = ?
	`, userID, shelfID, digest, index)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func finalizeUpload(db *sql.DB, p Principal, shelfID int64, digest string) (StoreBlobResponse, error) {
	manifest, physicalPendingBytes, err := uploadSessionManifest(db, p.UserID, shelfID, digest)
	if err != nil {
		return StoreBlobResponse{}, err
	}

	status, err := uploadStatus(db, p.UserID, shelfID, digest)
	if err != nil {
		return StoreBlobResponse{}, err
	}
	if !status.Complete {
		return StoreBlobResponse{}, fmt.Errorf("upload incomplete")
	}

	h := blake3.New(32, nil)
	if err := copyManifestBlob(h, manifest); err != nil {
		return StoreBlobResponse{}, err
	}
	if got := encodeDigest(h.Sum(nil)); got != manifest.Digest {
		return StoreBlobResponse{}, fmt.Errorf("final digest mismatch")
	}

	if err := insertManifest(db, manifest); err != nil {
		return StoreBlobResponse{}, err
	}

	exists, err := createOrRestoreRef(db, p, shelfID, manifest.Digest, manifest.Size)
	if err != nil {
		return StoreBlobResponse{}, err
	}
	if physicalPendingBytes > 0 {
		if err := addPendingBytes(db, p.UserID, physicalPendingBytes); err != nil {
			return StoreBlobResponse{}, err
		}
		if err := addShelfPendingBytes(db, shelfID, physicalPendingBytes); err != nil {
			return StoreBlobResponse{}, err
		}
	}
	if _, err := db.Exec("DELETE FROM upload_session_chunks WHERE user_id = ? AND shelf_id = ? AND digest = ?", p.UserID, shelfID, manifest.Digest); err != nil {
		return StoreBlobResponse{}, err
	}
	if _, err := db.Exec("DELETE FROM upload_sessions WHERE user_id = ? AND shelf_id = ? AND digest = ?", p.UserID, shelfID, manifest.Digest); err != nil {
		return StoreBlobResponse{}, err
	}

	return StoreBlobResponse{
		Digest: manifest.Digest,
		Size:   manifest.Size,
		Exists: exists,
	}, nil
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

		maxSimpleBytes, err := userMaxSimpleUploadBytes(db, p.UserID)
		if err != nil {
			http.Error(w, "failed to check simple upload limit", http.StatusInternalServerError)
			return
		}
		if r.ContentLength > maxSimpleBytes {
			http.Error(w, fmt.Sprintf("%s: limit %d bytes", errSimpleUploadTooLarge, maxSimpleBytes), http.StatusRequestEntityTooLarge)
			return
		}

		manifest, payloads, err := readSimpleManifest(r.Body, maxSimpleBytes)
		if err != nil {
			if errors.Is(err, errSimpleUploadTooLarge) {
				http.Error(w, fmt.Sprintf("%s: limit %d bytes", errSimpleUploadTooLarge, maxSimpleBytes), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "upload failed", http.StatusBadRequest)
			return
		}

		shelfName := requestShelfName(r)
		shelf, err := resolveShelf(db, p, shelfName, true)
		if err != nil {
			if shelfName == "" {
				exists, existsErr := userHasCleanBlob(db, p.UserID, 0, manifest.Digest)
				if existsErr != nil {
					http.Error(w, "failed to check blob ref", http.StatusInternalServerError)
					return
				}
				if exists {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(StoreBlobResponse{
						Digest: manifest.Digest,
						Size:   manifest.Size,
						Exists: true,
					})
					return
				}
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		exists, err := storeSimpleManifest(db, p, shelf.ID, manifest, payloads)
		if err != nil {
			if strings.Contains(err.Error(), "quota exceeded") {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "failed to store blob", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(StoreBlobResponse{
			Digest: manifest.Digest,
			Size:   manifest.Size,
			Exists: exists,
		})
	}
}

func handleRefsGet(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p := getPrincipal(r)
	var shelfID int64
	if name := requestShelfName(r); name != "" {
		shelf, err := resolveShelf(db, p, name, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		shelfID = shelf.ID
	}
	refs, err := userBlobRefs(db, p.UserID, shelfID)
	if err != nil {
		http.Error(w, "failed to list refs", http.StatusInternalServerError)
		return
	}
	if refs == nil {
		refs = []BlobRef{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ListRefsResponse{Refs: refs})
}

func handleQuotaGet(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p := getPrincipal(r)
	usedBytes, err := userStorageBytes(db, p.UserID)
	if err != nil {
		http.Error(w, "failed to read quota", http.StatusInternalServerError)
		return
	}
	maxBytes, err := userMaxBytes(db, p.UserID)
	if err != nil {
		http.Error(w, "failed to read quota", http.StatusInternalServerError)
		return
	}
	pendingBytes, maxPendingBytes, err := userPendingBytes(db, p.UserID)
	if err != nil {
		http.Error(w, "failed to read quota", http.StatusInternalServerError)
		return
	}
	reservedBytes, err := userUploadReservedBytes(db, p.UserID, "")
	if err != nil {
		http.Error(w, "failed to read quota", http.StatusInternalServerError)
		return
	}
	cleanDigestCount, err := userCleanDigestCount(db, p.UserID)
	if err != nil {
		http.Error(w, "failed to read quota", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QuotaResponse{
		UsedBytes:           usedBytes,
		MaxBytes:            maxBytes,
		PendingBytes:        pendingBytes,
		MaxPendingBytes:     maxPendingBytes,
		UploadReservedBytes: reservedBytes,
		CleanDigestCount:    cleanDigestCount,
	})
}

func userByName(db *sql.DB, name string) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT id FROM users WHERE name = ? AND enabled = 1", name).Scan(&id)
	return id, err
}

func aliasNamespaceByName(db *sql.DB, name string) (AliasNamespace, error) {
	var ns AliasNamespace
	err := db.QueryRow("SELECT id, name, owner_user_id FROM alias_namespaces WHERE name = ?", name).Scan(&ns.ID, &ns.Name, &ns.OwnerID)
	return ns, err
}

func effectiveAliasRole(db *sql.DB, namespaceID, userID int64, path string) (string, error) {
	rows, err := db.Query("SELECT path, role FROM alias_grants WHERE namespace_id = ? AND user_id = ?", namespaceID, userID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	bestRole := ""
	bestRank := 0
	for rows.Next() {
		var grantPath, role string
		if err := rows.Scan(&grantPath, &role); err != nil {
			return "", err
		}
		if grantPath != "" && path != grantPath && !strings.HasPrefix(path, grantPath+"/") {
			continue
		}
		if rank := roleRank(role); rank > bestRank {
			bestRank = rank
			bestRole = role
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return bestRole, nil
}

func requireAliasRole(db *sql.DB, ns AliasNamespace, userID int64, path string, required string) error {
	role, err := effectiveAliasRole(db, ns.ID, userID, path)
	if err != nil {
		return err
	}
	if roleRank(role) < roleRank(required) {
		return fmt.Errorf("alias permission denied")
	}
	return nil
}

func userAliasNamespaces(db *sql.DB, userID int64) ([]AliasNamespace, error) {
	rows, err := db.Query(`
		SELECT DISTINCT alias_namespaces.id, alias_namespaces.name, alias_namespaces.owner_user_id
		FROM alias_namespaces
		JOIN alias_grants ON alias_grants.namespace_id = alias_namespaces.id
		WHERE alias_grants.user_id = ?
		ORDER BY alias_namespaces.name ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var namespaces []AliasNamespace
	for rows.Next() {
		var ns AliasNamespace
		if err := rows.Scan(&ns.ID, &ns.Name, &ns.OwnerID); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, ns)
	}
	return namespaces, rows.Err()
}

func createAliasNamespace(db *sql.DB, p Principal, req AliasNamespaceCreateRequest) (AliasNamespace, error) {
	if err := validateAliasNamespaceName(req.Name); err != nil {
		return AliasNamespace{}, err
	}
	tx, err := db.Begin()
	if err != nil {
		return AliasNamespace{}, err
	}
	defer tx.Rollback()
	result, err := tx.Exec("INSERT INTO alias_namespaces (name, owner_user_id) VALUES (?, ?)", req.Name, p.UserID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return AliasNamespace{}, fmt.Errorf("alias namespace already exists")
		}
		return AliasNamespace{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return AliasNamespace{}, err
	}
	if _, err := tx.Exec("INSERT INTO alias_grants (namespace_id, path, user_id, role) VALUES (?, '', ?, 'admin')", id, p.UserID); err != nil {
		return AliasNamespace{}, err
	}
	if err := tx.Commit(); err != nil {
		return AliasNamespace{}, err
	}
	return AliasNamespace{ID: id, Name: req.Name, OwnerID: p.UserID}, nil
}

func listAliases(db *sql.DB, ns AliasNamespace, userID int64, prefix string) ([]Alias, error) {
	role, err := effectiveAliasRole(db, ns.ID, userID, prefix)
	if err != nil {
		return nil, err
	}
	if roleRank(role) < roleRank("read") {
		return nil, fmt.Errorf("alias permission denied")
	}
	query := `
		SELECT aliases.path, alias_versions.digest, aliases.current_version_id, aliases.updated_at
		FROM aliases
		JOIN alias_versions ON alias_versions.id = aliases.current_version_id
		WHERE aliases.namespace_id = ?
	`
	args := []any{ns.ID}
	if prefix != "" {
		query += " AND (aliases.path = ? OR aliases.path LIKE ?)"
		args = append(args, prefix, prefix+"/%")
	}
	query += " ORDER BY aliases.path ASC"
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var aliases []Alias
	for rows.Next() {
		var a Alias
		a.Namespace = ns.Name
		if err := rows.Scan(&a.Path, &a.Digest, &a.CurrentVersionID, &a.UpdatedAt); err != nil {
			return nil, err
		}
		if err := requireAliasRole(db, ns, userID, a.Path, "read"); err != nil {
			continue
		}
		aliases = append(aliases, a)
	}
	return aliases, rows.Err()
}

func getAlias(db *sql.DB, ns AliasNamespace, userID int64, path string) (Alias, error) {
	if err := requireAliasRole(db, ns, userID, path, "read"); err != nil {
		return Alias{}, err
	}
	var a Alias
	a.Namespace = ns.Name
	err := db.QueryRow(`
		SELECT aliases.path, alias_versions.digest, aliases.current_version_id, aliases.updated_at
		FROM aliases
		JOIN alias_versions ON alias_versions.id = aliases.current_version_id
		WHERE aliases.namespace_id = ? AND aliases.path = ?
	`, ns.ID, path).Scan(&a.Path, &a.Digest, &a.CurrentVersionID, &a.UpdatedAt)
	return a, err
}

func aliasVersions(db *sql.DB, ns AliasNamespace, userID int64, path string) ([]AliasVersion, error) {
	if err := requireAliasRole(db, ns, userID, path, "read"); err != nil {
		return nil, err
	}
	rows, err := db.Query(`
		SELECT alias_versions.id, alias_versions.digest, alias_versions.author_user_id, users.name,
		       alias_versions.previous_version_id, alias_versions.message, alias_versions.created_at
		FROM alias_versions
		JOIN users ON users.id = alias_versions.author_user_id
		WHERE alias_versions.namespace_id = ? AND alias_versions.path = ?
		ORDER BY alias_versions.id DESC
	`, ns.ID, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []AliasVersion
	for rows.Next() {
		var v AliasVersion
		var prev sql.NullInt64
		var message sql.NullString
		v.Namespace = ns.Name
		v.Path = path
		if err := rows.Scan(&v.ID, &v.Digest, &v.AuthorUserID, &v.AuthorUser, &prev, &message, &v.CreatedAt); err != nil {
			return nil, err
		}
		if prev.Valid {
			v.PreviousVersionID = &prev.Int64
		}
		if message.Valid {
			v.Message = &message.String
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

func createAliasVersion(db *sql.DB, p Principal, ns AliasNamespace, path string, req AliasVersionCreateRequest) (AliasVersion, *AliasConflictResponse, error) {
	if !validDigest(req.Digest) {
		return AliasVersion{}, nil, fmt.Errorf("invalid digest")
	}
	exists, err := userHasCleanBlob(db, p.UserID, 0, req.Digest)
	if err != nil {
		return AliasVersion{}, nil, err
	}
	if !exists {
		return AliasVersion{}, nil, fmt.Errorf("blob not found")
	}
	if err := requireAliasRole(db, ns, p.UserID, path, "write"); err != nil {
		return AliasVersion{}, nil, err
	}
	if _, err := manifestForDigest(db, req.Digest); err != nil {
		if err == sql.ErrNoRows {
			return AliasVersion{}, nil, fmt.Errorf("blob not found")
		}
		return AliasVersion{}, nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return AliasVersion{}, nil, err
	}
	defer tx.Rollback()

	var currentVersion int64
	err = tx.QueryRow("SELECT current_version_id FROM aliases WHERE namespace_id = ? AND path = ?", ns.ID, path).Scan(&currentVersion)
	if err == sql.ErrNoRows {
		if req.ExpectedVersion != nil {
			return AliasVersion{}, &AliasConflictResponse{Error: "alias conflict", CurrentVersion: 0}, nil
		}
	} else if err != nil {
		return AliasVersion{}, nil, err
	} else {
		if req.ExpectedVersion == nil || *req.ExpectedVersion != currentVersion {
			return AliasVersion{}, &AliasConflictResponse{Error: "alias conflict", CurrentVersion: currentVersion}, nil
		}
	}

	var msg any
	if strings.TrimSpace(req.Message) != "" {
		msg = strings.TrimSpace(req.Message)
	}
	var prev any
	if currentVersion != 0 {
		prev = currentVersion
	}
	result, err := tx.Exec(`
		INSERT INTO alias_versions (namespace_id, path, digest, author_user_id, previous_version_id, message)
		VALUES (?, ?, ?, ?, ?, ?)
	`, ns.ID, path, req.Digest, p.UserID, prev, msg)
	if err != nil {
		return AliasVersion{}, nil, err
	}
	versionID, err := result.LastInsertId()
	if err != nil {
		return AliasVersion{}, nil, err
	}
	if currentVersion == 0 {
		if _, err := tx.Exec("INSERT INTO aliases (namespace_id, path, current_version_id) VALUES (?, ?, ?)", ns.ID, path, versionID); err != nil {
			return AliasVersion{}, nil, err
		}
	} else if _, err := tx.Exec("UPDATE aliases SET current_version_id = ?, updated_at = CURRENT_TIMESTAMP WHERE namespace_id = ? AND path = ?", versionID, ns.ID, path); err != nil {
		return AliasVersion{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return AliasVersion{}, nil, err
	}
	versions, err := aliasVersions(db, ns, p.UserID, path)
	if err != nil {
		return AliasVersion{}, nil, err
	}
	if len(versions) == 0 {
		return AliasVersion{}, nil, sql.ErrNoRows
	}
	return versions[0], nil, nil
}

func upsertAliasGrant(db *sql.DB, ns AliasNamespace, p Principal, req AliasGrantRequest) (AliasGrant, error) {
	grantPath, err := normalizeAliasGrantPath(req.Path)
	if err != nil {
		return AliasGrant{}, err
	}
	if err := requireAliasRole(db, ns, p.UserID, grantPath, "admin"); err != nil {
		return AliasGrant{}, err
	}
	if err := validateAliasRole(req.Role); err != nil {
		return AliasGrant{}, err
	}
	userID, err := userByName(db, req.User)
	if err != nil {
		return AliasGrant{}, err
	}
	_, err = db.Exec(`
		INSERT INTO alias_grants (namespace_id, path, user_id, role)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(namespace_id, path, user_id) DO UPDATE SET role = excluded.role
	`, ns.ID, grantPath, userID, req.Role)
	if err != nil {
		return AliasGrant{}, err
	}
	displayPath := grantPath
	if displayPath == "" {
		displayPath = "/"
	}
	return AliasGrant{Namespace: ns.Name, Path: displayPath, User: req.User, Role: req.Role}, nil
}

func deleteAliasGrant(db *sql.DB, ns AliasNamespace, p Principal, req AliasGrantRequest) error {
	grantPath, err := normalizeAliasGrantPath(req.Path)
	if err != nil {
		return err
	}
	if err := requireAliasRole(db, ns, p.UserID, grantPath, "admin"); err != nil {
		return err
	}
	userID, err := userByName(db, req.User)
	if err != nil {
		return err
	}
	_, err = db.Exec("DELETE FROM alias_grants WHERE namespace_id = ? AND path = ? AND user_id = ?", ns.ID, grantPath, userID)
	return err
}

func userShelves(db *sql.DB, userID int64) ([]Shelf, error) {
	rows, err := db.Query(`
		SELECT
			shelves.id,
			shelves.name,
			shelves.max_bytes,
			COALESCE(SUM(CASE WHEN blob_refs.dirty = 0 THEN blob_refs.size ELSE 0 END), 0) AS used_bytes,
			shelves.max_pending_bytes,
			shelves.pending_bytes,
			COUNT(blob_refs.digest) AS ref_count,
			shelves.enabled,
			shelves.is_default
		FROM shelves
		LEFT JOIN blob_refs ON blob_refs.shelf_id = shelves.id
		WHERE shelves.user_id = ?
		GROUP BY shelves.id
		ORDER BY shelves.name ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shelves []Shelf
	for rows.Next() {
		var s Shelf
		var enabled, isDefault int
		if err := rows.Scan(&s.ID, &s.Name, &s.MaxBytes, &s.UsedBytes, &s.MaxPendingBytes, &s.PendingBytes, &s.RefCount, &enabled, &isDefault); err != nil {
			return nil, err
		}
		s.Enabled = enabled != 0
		s.IsDefault = isDefault != 0
		shelves = append(shelves, s)
	}
	return shelves, rows.Err()
}

func createShelf(db *sql.DB, userID int64, req ShelfCreateRequest) (Shelf, error) {
	if err := validateNewShelfName(req.Name); err != nil {
		return Shelf{}, err
	}
	if req.MaxBytes <= 0 || req.MaxPendingBytes <= 0 {
		return Shelf{}, fmt.Errorf("shelf quotas must be positive")
	}

	result, err := db.Exec(`
		INSERT INTO shelves (user_id, name, max_bytes, max_pending_bytes)
		VALUES (?, ?, ?, ?)
	`, userID, req.Name, req.MaxBytes, req.MaxPendingBytes)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return Shelf{}, fmt.Errorf("shelf already exists")
		}
		return Shelf{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Shelf{}, err
	}
	if req.Name != defaultShelfName {
		if _, err := db.Exec("UPDATE users SET multi_shelf_enabled = 1 WHERE id = ?", userID); err != nil {
			return Shelf{}, err
		}
	}
	return Shelf{
		ID:              id,
		Name:            req.Name,
		MaxBytes:        req.MaxBytes,
		UsedBytes:       0,
		MaxPendingBytes: req.MaxPendingBytes,
		PendingBytes:    0,
		RefCount:        0,
		Enabled:         true,
	}, nil
}

func renameShelf(db *sql.DB, userID int64, oldName, newName string) (Shelf, error) {
	if err := validateShelfName(oldName); err != nil {
		return Shelf{}, err
	}
	if err := validateNewShelfName(newName); err != nil {
		return Shelf{}, err
	}

	shelf, err := shelfByName(db, userID, oldName)
	if err != nil {
		return Shelf{}, err
	}
	if !shelf.Enabled {
		return Shelf{}, fmt.Errorf("shelf disabled")
	}

	_, err = db.Exec("UPDATE shelves SET name = ? WHERE id = ?", newName, shelf.ID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return Shelf{}, fmt.Errorf("shelf already exists")
		}
		return Shelf{}, err
	}

	return shelfByName(db, userID, newName)
}

func setDefaultShelf(db *sql.DB, userID int64, name string) (Shelf, error) {
	if err := validateShelfName(name); err != nil {
		return Shelf{}, err
	}

	shelf, err := shelfByName(db, userID, name)
	if err != nil {
		return Shelf{}, err
	}
	if !shelf.Enabled {
		return Shelf{}, fmt.Errorf("shelf disabled")
	}

	tx, err := db.Begin()
	if err != nil {
		return Shelf{}, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("UPDATE shelves SET is_default = 0 WHERE user_id = ?", userID); err != nil {
		return Shelf{}, err
	}
	if _, err := tx.Exec("UPDATE shelves SET is_default = 1 WHERE id = ?", shelf.ID); err != nil {
		return Shelf{}, err
	}
	if err := tx.Commit(); err != nil {
		return Shelf{}, err
	}

	return shelfByName(db, userID, name)
}

func deleteShelf(db *sql.DB, userID int64, name string, force bool) error {
	if !force {
		return fmt.Errorf("force required: retry with ?force=1 or run `sht shelf delete %s --force`", name)
	}
	if err := validateShelfName(name); err != nil {
		return err
	}

	shelf, err := shelfByName(db, userID, name)
	if err == sql.ErrNoRows {
		return err
	}
	if err != nil {
		return err
	}
	if shelf.IsDefault {
		return fmt.Errorf("cannot delete default shelf")
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM upload_session_chunks WHERE user_id = ? AND shelf_id = ?", userID, shelf.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM upload_sessions WHERE user_id = ? AND shelf_id = ?", userID, shelf.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM blob_refs WHERE user_id = ? AND shelf_id = ?", userID, shelf.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM shelves WHERE id = ?", shelf.ID); err != nil {
		return err
	}

	var nonDefaultCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM shelves WHERE user_id = ? AND is_default = 0", userID).Scan(&nonDefaultCount); err != nil {
		return err
	}
	if nonDefaultCount == 0 {
		if _, err := tx.Exec("UPDATE users SET multi_shelf_enabled = 0 WHERE id = ?", userID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func shelfHTTPStatus(err error) int {
	if err == sql.ErrNoRows {
		return http.StatusNotFound
	}
	if strings.Contains(err.Error(), "exists") {
		return http.StatusConflict
	}
	return http.StatusBadRequest
}

func handleShelfPath(db *sql.DB, p Principal, w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/shelves/"), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "invalid shelf name", http.StatusBadRequest)
		return
	}
	name := parts[0]
	if strings.Contains(name, "/") {
		http.Error(w, "invalid shelf name", http.StatusBadRequest)
		return
	}

	if len(parts) == 2 && parts[1] == "default" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		shelf, err := setDefaultShelf(db, p.UserID, name)
		if err != nil {
			http.Error(w, err.Error(), shelfHTTPStatus(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(shelf)
		return
	}

	if len(parts) != 1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodDelete:
		force := r.URL.Query().Get("force") == "1" || r.URL.Query().Get("force") == "true"
		if err := deleteShelf(db, p.UserID, name, force); err != nil {
			http.Error(w, err.Error(), shelfHTTPStatus(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		defer r.Body.Close()
		var req ShelfRenameRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad shelf request", http.StatusBadRequest)
			return
		}
		shelf, err := renameShelf(db, p.UserID, name, req.Name)
		if err != nil {
			http.Error(w, err.Error(), shelfHTTPStatus(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(shelf)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleShelves(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := getPrincipal(r)
		if strings.HasPrefix(r.URL.Path, "/shelves/") {
			handleShelfPath(db, p, w, r)
			return
		}

		if r.URL.Path != "/shelves" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			shelves, err := userShelves(db, p.UserID)
			if err != nil {
				http.Error(w, "failed to list shelves", http.StatusInternalServerError)
				return
			}
			if shelves == nil {
				shelves = []Shelf{}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ShelfListResponse{Shelves: shelves})
		case http.MethodPost:
			defer r.Body.Close()
			var req ShelfCreateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad shelf request", http.StatusBadRequest)
				return
			}
			shelf, err := createShelf(db, p.UserID, req)
			if err != nil {
				http.Error(w, err.Error(), shelfHTTPStatus(err))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(shelf)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func aliasHTTPStatus(err error) int {
	if err == sql.ErrNoRows {
		return http.StatusNotFound
	}
	msg := err.Error()
	if strings.Contains(msg, "permission denied") {
		return http.StatusForbidden
	}
	if strings.Contains(msg, "already exists") {
		return http.StatusConflict
	}
	if strings.Contains(msg, "not found") {
		return http.StatusNotFound
	}
	return http.StatusBadRequest
}

func handleAliasNamespaces(db *sql.DB, p Principal, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		namespaces, err := userAliasNamespaces(db, p.UserID)
		if err != nil {
			http.Error(w, "failed to list alias namespaces", http.StatusInternalServerError)
			return
		}
		if namespaces == nil {
			namespaces = []AliasNamespace{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AliasNamespaceListResponse{Namespaces: namespaces})
	case http.MethodPost:
		defer r.Body.Close()
		var req AliasNamespaceCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad alias namespace request", http.StatusBadRequest)
			return
		}
		ns, err := createAliasNamespace(db, p, req)
		if err != nil {
			http.Error(w, err.Error(), aliasHTTPStatus(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ns)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleAliasGrantPath(db *sql.DB, p Principal, ns AliasNamespace, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req AliasGrantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad alias grant request", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPost:
		grant, err := upsertAliasGrant(db, ns, p, req)
		if err != nil {
			http.Error(w, err.Error(), aliasHTTPStatus(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AliasGrantResponse{Grant: grant})
	case http.MethodDelete:
		if err := deleteAliasGrant(db, ns, p, req); err != nil {
			http.Error(w, err.Error(), aliasHTTPStatus(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleAliasBlobGet(db *sql.DB, p Principal, ns AliasNamespace, path string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	alias, err := getAlias(db, ns, p.UserID, path)
	if err != nil {
		http.Error(w, err.Error(), aliasHTTPStatus(err))
		return
	}
	manifest, err := manifestForDigest(db, alias.Digest)
	if err != nil {
		http.Error(w, "blob not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
	if r.Method == http.MethodHead {
		return
	}
	if err := copyManifestBlob(w, manifest); err != nil {
		http.Error(w, "failed to read blob", http.StatusInternalServerError)
	}
}

func handleAliasPath(db *sql.DB, p Principal, w http.ResponseWriter, r *http.Request) {
	trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/alias/"), "/")
	if trimmed == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	parts := strings.Split(trimmed, "/")
	nsName := parts[0]
	if err := validateAliasNamespaceName(nsName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ns, err := aliasNamespaceByName(db, nsName)
	if err != nil {
		http.Error(w, "alias namespace not found", http.StatusNotFound)
		return
	}

	if len(parts) == 2 && parts[1] == "grants" {
		handleAliasGrantPath(db, p, ns, w, r)
		return
	}

	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
		if prefix != "" {
			prefix, err = normalizeAliasGrantPath(prefix)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		aliases, err := listAliases(db, ns, p.UserID, prefix)
		if err != nil {
			http.Error(w, err.Error(), aliasHTTPStatus(err))
			return
		}
		if aliases == nil {
			aliases = []Alias{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AliasListResponse{Aliases: aliases})
		return
	}

	if len(parts) >= 3 && parts[len(parts)-1] == "versions" {
		path, err := normalizeAliasPath(strings.Join(parts[1:len(parts)-1], "/"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			versions, err := aliasVersions(db, ns, p.UserID, path)
			if err != nil {
				http.Error(w, err.Error(), aliasHTTPStatus(err))
				return
			}
			if versions == nil {
				versions = []AliasVersion{}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(AliasVersionListResponse{Versions: versions})
		case http.MethodPost:
			defer r.Body.Close()
			var req AliasVersionCreateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad alias version request", http.StatusBadRequest)
				return
			}
			version, conflict, err := createAliasVersion(db, p, ns, path, req)
			if conflict != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(conflict)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), aliasHTTPStatus(err))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(version)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) >= 3 && parts[len(parts)-1] == "blob" {
		path, err := normalizeAliasPath(strings.Join(parts[1:len(parts)-1], "/"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		handleAliasBlobGet(db, p, ns, path, w, r)
		return
	}

	path, err := normalizeAliasPath(strings.Join(parts[1:], "/"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	alias, err := getAlias(db, ns, p.UserID, path)
	if err != nil {
		http.Error(w, err.Error(), aliasHTTPStatus(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alias)
}

func handleAliases(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := getPrincipal(r)
		if r.URL.Path == "/alias/namespaces" {
			handleAliasNamespaces(db, p, w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/alias/") {
			handleAliasPath(db, p, w, r)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func ensureSingleDefaultShelves(db *sql.DB) error {
	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		return err
	}
	defer rows.Close()

	var userIDs []int64
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, userID := range userIDs {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM shelves WHERE user_id = ? AND is_default = 1", userID).Scan(&count); err != nil {
			return err
		}
		if count == 1 {
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE shelves SET is_default = 0 WHERE user_id = ?", userID); err != nil {
			tx.Rollback()
			return err
		}
		var shelfID int64
		err = tx.QueryRow(`
			SELECT id
			FROM shelves
			WHERE user_id = ?
			ORDER BY CASE WHEN name = 'main' THEN 0 WHEN name = 'default' THEN 1 ELSE 2 END, id ASC
			LIMIT 1
		`, userID).Scan(&shelfID)
		if err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.Exec("UPDATE shelves SET is_default = 1 WHERE id = ?", shelfID); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func handleUploadsPost(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		var manifest ManifestRequest
		if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
			http.Error(w, "bad manifest", http.StatusBadRequest)
			return
		}
		manifest, err := normalizeManifest(manifest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		p := getPrincipal(r)
		shelf, err := resolveShelf(db, p, requestShelfName(r), true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status, err := createUploadSession(db, p, shelf.ID, manifest)
		if err != nil {
			if strings.Contains(err.Error(), "quota exceeded") {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			if strings.Contains(err.Error(), "different manifest") {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, "failed to create upload session", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}
}

func handleUploadPath(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	p := getPrincipal(r)
	shelf, err := resolveShelf(db, p, requestShelfName(r), true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/uploads/"), "/")
	if len(parts) == 0 || parts[0] == "" || !validDigest(parts[0]) {
		http.Error(w, "invalid upload digest", http.StatusBadRequest)
		return
	}
	digest := parts[0]

	if len(parts) == 1 && r.Method == http.MethodGet {
		status, err := uploadStatus(db, p.UserID, shelf.ID, digest)
		if err == sql.ErrNoRows {
			http.Error(w, "upload not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "failed to get upload status", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
		return
	}

	if len(parts) == 2 && parts[1] == "finalize" && r.Method == http.MethodPost {
		out, err := finalizeUpload(db, p, shelf.ID, digest)
		if err == sql.ErrNoRows {
			http.Error(w, "upload not found", http.StatusNotFound)
			return
		}
		if err != nil {
			if strings.Contains(err.Error(), "incomplete") || strings.Contains(err.Error(), "digest mismatch") {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, "failed to finalize upload", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
		return
	}

	if len(parts) == 3 && parts[1] == "chunks" && r.Method == http.MethodPut {
		index, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || index < 0 {
			http.Error(w, "invalid chunk index", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var chunk ManifestChunk
		err = db.QueryRow(`
			SELECT chunk_index, chunk_digest, size
			FROM upload_session_chunks
			WHERE user_id = ? AND shelf_id = ? AND digest = ? AND chunk_index = ?
		`, p.UserID, shelf.ID, digest, index).Scan(&chunk.Index, &chunk.Digest, &chunk.Size)
		if err == sql.ErrNoRows {
			http.Error(w, "chunk not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "failed to load chunk", http.StatusInternalServerError)
			return
		}

		data, err := io.ReadAll(io.LimitReader(r.Body, chunk.Size+1))
		if err != nil {
			http.Error(w, "failed to read chunk", http.StatusBadRequest)
			return
		}
		if int64(len(data)) != chunk.Size {
			http.Error(w, "chunk size mismatch", http.StatusBadRequest)
			return
		}
		sum := blake3.Sum256(data)
		if got := encodeDigest(sum[:]); got != chunk.Digest {
			http.Error(w, "chunk digest mismatch", http.StatusBadRequest)
			return
		}

		existed, err := writeChunk(chunk.Digest, data)
		if err != nil {
			http.Error(w, "failed to store chunk", http.StatusInternalServerError)
			return
		}
		var physicalBytes int64
		if !existed {
			physicalBytes = chunk.Size
		}
		if err := markUploadChunk(db, p.UserID, shelf.ID, digest, index, physicalBytes); err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "chunk not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to mark chunk uploaded", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ChunkUploadResponse{
			Digest:      digest,
			Index:       index,
			ChunkDigest: chunk.Digest,
			Size:        chunk.Size,
			Exists:      existed,
		})
		return
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func handleBlobPath(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p := getPrincipal(r)
	fmt.Println("user:", p.UserName)
	var shelfID int64
	if name := requestShelfName(r); name != "" {
		shelf, err := resolveShelf(db, p, name, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		shelfID = shelf.ID
	}

	digest := strings.TrimPrefix(r.URL.Path, "/blob/")
	if digest == "" || strings.Contains(digest, "/") || strings.Contains(digest, "..") {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	exists, err := userHasCleanBlob(db, p.UserID, shelfID, digest)
	if err != nil {
		http.Error(w, "failed to check blob ref", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "blob not found", http.StatusNotFound)
		return
	}

	if r.Method == http.MethodDelete {
		shelf, err := resolveShelf(db, p, requestShelfName(r), true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		released, err := releaseBlobRef(db, p.UserID, shelf.ID, digest)
		if err != nil {
			http.Error(w, "failed to release blob", http.StatusInternalServerError)
			return
		}
		if !released {
			http.Error(w, "blob not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	manifest, err := manifestForDigest(db, digest)
	if err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
		if r.Method == http.MethodHead {
			return
		}
		if err := copyManifestBlob(w, manifest); err != nil {
			http.Error(w, "failed to read blob", http.StatusInternalServerError)
		}
		return
	}
	if err != sql.ErrNoRows {
		http.Error(w, "failed to read blob manifest", http.StatusInternalServerError)
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
	max_pending_bytes INTEGER NOT NULL DEFAULT %d,
	pending_bytes INTEGER NOT NULL DEFAULT 0,
	max_simple_upload_bytes INTEGER NOT NULL DEFAULT %d,
	multi_shelf_enabled INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shelves (
	id INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	is_default INTEGER NOT NULL DEFAULT 0,
	max_bytes INTEGER NOT NULL,
	max_pending_bytes INTEGER NOT NULL,
	pending_bytes INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	UNIQUE(user_id, name),
	FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS key_ids (
	id INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	public_key TEXT,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS blob_refs (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	dirty INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, shelf_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(shelf_id) REFERENCES shelves(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);

CREATE INDEX IF NOT EXISTS blob_refs_user_id ON blob_refs(user_id);
CREATE INDEX IF NOT EXISTS blob_refs_digest ON blob_refs(digest);

CREATE TABLE IF NOT EXISTS blob_manifests (
	digest TEXT PRIMARY KEY,
	size INTEGER NOT NULL,
	chunk_size INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS blob_manifest_chunks (
	digest TEXT NOT NULL,
	chunk_index INTEGER NOT NULL,
	chunk_digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	PRIMARY KEY (digest, chunk_index),
	FOREIGN KEY(digest) REFERENCES blob_manifests(digest)
);
CREATE INDEX IF NOT EXISTS blob_manifest_chunks_chunk_digest ON blob_manifest_chunks(chunk_digest);

CREATE TABLE IF NOT EXISTS upload_sessions (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	chunk_size INTEGER NOT NULL,
	physical_pending_bytes INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (user_id, shelf_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(shelf_id) REFERENCES shelves(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);
CREATE TABLE IF NOT EXISTS upload_session_chunks (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	chunk_index INTEGER NOT NULL,
	chunk_digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	uploaded INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, shelf_id, digest, chunk_index),
	FOREIGN KEY(user_id, shelf_id, digest) REFERENCES upload_sessions(user_id, shelf_id, digest)
);
CREATE INDEX IF NOT EXISTS upload_session_chunks_digest ON upload_session_chunks(digest);

CREATE TABLE IF NOT EXISTS alias_namespaces (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	owner_user_id INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY(owner_user_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS alias_grants (
	namespace_id INTEGER NOT NULL,
	path TEXT NOT NULL,
	user_id INTEGER NOT NULL,
	role TEXT NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (namespace_id, path, user_id),
	FOREIGN KEY(namespace_id) REFERENCES alias_namespaces(id),
	FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS alias_grants_user_id ON alias_grants(user_id);
CREATE TABLE IF NOT EXISTS aliases (
	namespace_id INTEGER NOT NULL,
	path TEXT NOT NULL,
	current_version_id INTEGER NOT NULL,
	updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (namespace_id, path),
	FOREIGN KEY(namespace_id) REFERENCES alias_namespaces(id)
);
CREATE TABLE IF NOT EXISTS alias_versions (
	id INTEGER PRIMARY KEY,
	namespace_id INTEGER NOT NULL,
	path TEXT NOT NULL,
	digest TEXT NOT NULL,
	author_user_id INTEGER NOT NULL,
	previous_version_id INTEGER,
	message TEXT,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY(namespace_id) REFERENCES alias_namespaces(id),
	FOREIGN KEY(author_user_id) REFERENCES users(id),
	FOREIGN KEY(previous_version_id) REFERENCES alias_versions(id)
);
CREATE INDEX IF NOT EXISTS alias_versions_namespace_path ON alias_versions(namespace_id, path);
`, defaultUserMaxBytes, defaultUserMaxPendingBytes, defaultMaxSimpleUploadBytes))
	if err != nil {
		log.Fatal(err)
	}

	if err := ensureColumn(db, "users", "max_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultUserMaxBytes)); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "users", "max_pending_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultUserMaxPendingBytes)); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "users", "pending_bytes", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "users", "max_simple_upload_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultMaxSimpleUploadBytes)); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "users", "multi_shelf_enabled", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "shelves", "is_default", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "key_ids", "public_key", "TEXT"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE UNIQUE INDEX IF NOT EXISTS key_ids_public_key_unique
ON key_ids(public_key)
WHERE public_key IS NOT NULL
`); err != nil {
		log.Fatal(err)
	}
	if err := migrateShelves(db); err != nil {
		log.Fatal(err)
	}
	if err := migrateAliases(db); err != nil {
		log.Fatal(err)
	}

	return db
}

func migrateAliases(db *sql.DB) error {
	if _, err := db.Exec(`
INSERT OR IGNORE INTO alias_namespaces (name, owner_user_id)
SELECT name, id FROM users;
INSERT OR IGNORE INTO alias_grants (namespace_id, path, user_id, role)
SELECT alias_namespaces.id, '', users.id, 'admin'
FROM users
JOIN alias_namespaces ON alias_namespaces.name = users.name;
`); err != nil {
		return err
	}
	return nil
}

func migrateShelves(db *sql.DB) error {
	if _, err := db.Exec(`
UPDATE shelves
SET name = 'main'
WHERE name = 'default'
  AND is_default = 1
  AND NOT EXISTS (
    SELECT 1
    FROM shelves AS existing
    WHERE existing.user_id = shelves.user_id
      AND existing.name = 'main'
  );
`); err != nil {
		return err
	}
	if _, err := db.Exec(`
INSERT OR IGNORE INTO shelves (user_id, name, is_default, max_bytes, max_pending_bytes, pending_bytes)
SELECT users.id, 'main', 1, users.max_bytes, users.max_pending_bytes, users.pending_bytes
FROM users
WHERE NOT EXISTS (
  SELECT 1
  FROM shelves
  WHERE shelves.user_id = users.id
);
`); err != nil {
		return err
	}
	if err := ensureSingleDefaultShelves(db); err != nil {
		return err
	}

	hasShelfID, err := hasColumn(db, "blob_refs", "shelf_id")
	if err != nil {
		return err
	}
	if !hasShelfID {
		if _, err := db.Exec(`
ALTER TABLE blob_refs RENAME TO blob_refs_old;
CREATE TABLE blob_refs (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	dirty INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, shelf_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(shelf_id) REFERENCES shelves(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);
INSERT INTO blob_refs (user_id, shelf_id, key_id, digest, size, created_at, dirty)
SELECT old.user_id, shelves.id, old.key_id, old.digest, old.size, old.created_at, old.dirty
FROM blob_refs_old AS old
JOIN shelves ON shelves.user_id = old.user_id AND shelves.is_default = 1;
DROP TABLE blob_refs_old;
CREATE INDEX IF NOT EXISTS blob_refs_user_id ON blob_refs(user_id);
CREATE INDEX IF NOT EXISTS blob_refs_digest ON blob_refs(digest);
`); err != nil {
			return err
		}
	} else {
		if err := ensureColumn(db, "blob_refs", "dirty", "INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}

	hasUploadShelfID, err := hasColumn(db, "upload_sessions", "shelf_id")
	if err != nil {
		return err
	}
	if !hasUploadShelfID {
		if _, err := db.Exec(`
ALTER TABLE upload_sessions RENAME TO upload_sessions_old;
ALTER TABLE upload_session_chunks RENAME TO upload_session_chunks_old;
CREATE TABLE upload_sessions (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	chunk_size INTEGER NOT NULL,
	physical_pending_bytes INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (user_id, shelf_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(shelf_id) REFERENCES shelves(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);
CREATE TABLE upload_session_chunks (
	user_id INTEGER NOT NULL,
	shelf_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	chunk_index INTEGER NOT NULL,
	chunk_digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	uploaded INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, shelf_id, digest, chunk_index),
	FOREIGN KEY(user_id, shelf_id, digest) REFERENCES upload_sessions(user_id, shelf_id, digest)
);
INSERT INTO upload_sessions (user_id, shelf_id, key_id, digest, size, chunk_size, physical_pending_bytes, created_at, updated_at)
SELECT old.user_id, shelves.id, old.key_id, old.digest, old.size, old.chunk_size, old.physical_pending_bytes, old.created_at, old.updated_at
FROM upload_sessions_old AS old
JOIN shelves ON shelves.user_id = old.user_id AND shelves.is_default = 1;
INSERT INTO upload_session_chunks (user_id, shelf_id, digest, chunk_index, chunk_digest, size, uploaded)
SELECT old.user_id, shelves.id, old.digest, old.chunk_index, old.chunk_digest, old.size, old.uploaded
FROM upload_session_chunks_old AS old
JOIN shelves ON shelves.user_id = old.user_id AND shelves.is_default = 1;
DROP TABLE upload_sessions_old;
DROP TABLE upload_session_chunks_old;
CREATE INDEX IF NOT EXISTS upload_session_chunks_digest ON upload_session_chunks(digest);
`); err != nil {
			return err
		}
	} else if err := ensureColumn(db, "upload_sessions", "physical_pending_bytes", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}

	return nil
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

	if err := os.MkdirAll(blobDir, 0755); err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		log.Fatal(err)
	}

	db := openDB(dbPath)
	defer db.Close()

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

	mux := http.NewServeMux()

	mux.HandleFunc("/blob", requirePrincipal(db, handleBlobPost(db)))
	mux.HandleFunc("/uploads", requirePrincipal(db, handleUploadsPost(db)))
	mux.HandleFunc("/shelves", requirePrincipal(db, handleShelves(db)))
	mux.HandleFunc("/shelves/", requirePrincipal(db, handleShelves(db)))
	mux.HandleFunc("/alias", requirePrincipal(db, handleAliases(db)))
	mux.HandleFunc("/alias/", requirePrincipal(db, handleAliases(db)))

	mux.HandleFunc("/quota", requirePrincipal(db, func(w http.ResponseWriter, r *http.Request) {
		handleQuotaGet(db, w, r)
	}))

	mux.HandleFunc("/refs", requirePrincipal(db, func(w http.ResponseWriter, r *http.Request) {
		handleRefsGet(db, w, r)
	}))

	mux.HandleFunc("/uploads/", requirePrincipal(db, func(w http.ResponseWriter, r *http.Request) {
		handleUploadPath(db, w, r)
	}))

	mux.HandleFunc("/blob/", requirePrincipal(db, func(w http.ResponseWriter, r *http.Request) {
		handleBlobPath(db, w, r)
	}))

	log.Println("Listening on ", path)
	log.Fatal(http.Serve(ln, mux))
}
