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

var errSimpleUploadTooLarge = errors.New("simple upload too large; use manifest/resumable upload")

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

func userStorageBytes(db *sql.DB, userID int64) (int64, error) {
	var size int64
	err := db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM blob_refs WHERE user_id = ? AND dirty = 0", userID).Scan(&size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func userUploadReservedBytes(db *sql.DB, userID int64, excludeDigest string) (int64, error) {
	var size int64
	var err error
	if excludeDigest == "" {
		err = db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM upload_sessions WHERE user_id = ?", userID).Scan(&size)
	} else {
		err = db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM upload_sessions WHERE user_id = ? AND digest != ?", userID, excludeDigest).Scan(&size)
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

func userBlobRefState(db *sql.DB, userID int64, digest string) (bool, bool, error) {
	var dirty int
	err := db.QueryRow("SELECT dirty FROM blob_refs WHERE user_id = ? AND digest = ?", userID, digest).Scan(&dirty)
	if err == sql.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, dirty != 0, nil
}

func userHasCleanBlob(db *sql.DB, userID int64, digest string) (bool, error) {
	var exists int
	err := db.QueryRow("SELECT 1 FROM blob_refs WHERE user_id = ? AND digest = ? AND dirty = 0", userID, digest).Scan(&exists)
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

func restoreBlobRef(db *sql.DB, p Principal, digest string, size int64) error {
	_, err := db.Exec(
		"UPDATE blob_refs SET key_id = ?, size = ?, dirty = 0, created_at = CURRENT_TIMESTAMP WHERE user_id = ? AND digest = ?",
		p.KeyID,
		size,
		p.UserID,
		digest,
	)
	return err
}

func releaseBlobRef(db *sql.DB, userID int64, digest string) (bool, error) {
	result, err := db.Exec("UPDATE blob_refs SET dirty = 1 WHERE user_id = ? AND digest = ? AND dirty = 0", userID, digest)
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

func addPendingBytes(db *sql.DB, userID, uploadBytes int64) error {
	_, err := db.Exec("UPDATE users SET pending_bytes = pending_bytes + ? WHERE id = ?", uploadBytes, userID)
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

func userBlobRefs(db *sql.DB, userID int64) ([]BlobRef, error) {
	rows, err := db.Query(`
		SELECT digest, size, key_id, created_at, dirty
		FROM blob_refs
		WHERE user_id = ?
		ORDER BY created_at DESC, digest ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []BlobRef
	for rows.Next() {
		var ref BlobRef
		var dirty int
		if err := rows.Scan(&ref.Digest, &ref.Size, &ref.KeyID, &ref.CreatedAt, &dirty); err != nil {
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

func createOrRestoreRef(db *sql.DB, p Principal, digest string, size int64) (bool, error) {
	refExists, refDirty, err := userBlobRefState(db, p.UserID, digest)
	if err != nil {
		return false, err
	}
	if refExists && !refDirty {
		return true, nil
	}
	if refExists {
		if err := restoreBlobRef(db, p, digest, size); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := addBlobRef(db, p, digest, size); err != nil {
		return false, err
	}
	return false, nil
}

func storeSimpleManifest(db *sql.DB, p Principal, m ManifestRequest, payloads [][]byte) (bool, error) {
	refExists, refDirty, err := userBlobRefState(db, p.UserID, m.Digest)
	if err != nil {
		return false, err
	}
	if refExists && !refDirty {
		return true, nil
	}

	if err := checkUserQuota(db, p.UserID, m.Size); err != nil {
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
		}
	}

	_, err = createOrRestoreRef(db, p, m.Digest, m.Size)
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

func uploadSessionManifest(db *sql.DB, userID int64, digest string) (ManifestRequest, int64, error) {
	var m ManifestRequest
	var physicalPendingBytes int64
	err := db.QueryRow(
		"SELECT digest, size, chunk_size, physical_pending_bytes FROM upload_sessions WHERE user_id = ? AND digest = ?",
		userID,
		digest,
	).Scan(&m.Digest, &m.Size, &m.ChunkSize, &physicalPendingBytes)
	if err != nil {
		return ManifestRequest{}, 0, err
	}

	rows, err := db.Query(`
		SELECT chunk_index, chunk_digest, size
		FROM upload_session_chunks
		WHERE user_id = ? AND digest = ?
		ORDER BY chunk_index ASC
	`, userID, digest)
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

func uploadStatus(db *sql.DB, userID int64, digest string) (UploadStatusResponse, error) {
	manifest, physicalPendingBytes, err := uploadSessionManifest(db, userID, digest)
	if err != nil {
		return UploadStatusResponse{}, err
	}

	uploadedRows, err := db.Query(`
		SELECT chunk_index
		FROM upload_session_chunks
		WHERE user_id = ? AND digest = ? AND uploaded = 1
		ORDER BY chunk_index ASC
	`, userID, digest)
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

func createUploadSession(db *sql.DB, p Principal, manifest ManifestRequest) (UploadStatusResponse, error) {
	if exists, err := manifestExists(db, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	} else if exists {
		if err := checkUserQuota(db, p.UserID, manifest.Size); err != nil {
			return UploadStatusResponse{}, err
		}
		_, err := createOrRestoreRef(db, p, manifest.Digest, manifest.Size)
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

	existing, _, err := uploadSessionManifest(db, p.UserID, manifest.Digest)
	if err == nil {
		if !sameManifest(existing, manifest) {
			return UploadStatusResponse{}, fmt.Errorf("upload session already exists with different manifest")
		}
		return uploadStatus(db, p.UserID, manifest.Digest)
	}
	if err != sql.ErrNoRows {
		return UploadStatusResponse{}, err
	}

	if err := checkUserQuotaExcludingUpload(db, p.UserID, manifest.Size, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}
	if err := checkUserPendingQuotaExcludingUpload(db, p.UserID, manifest.Size, manifest.Digest); err != nil {
		return UploadStatusResponse{}, err
	}

	tx, err := db.Begin()
	if err != nil {
		return UploadStatusResponse{}, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		"INSERT INTO upload_sessions (user_id, key_id, digest, size, chunk_size) VALUES (?, ?, ?, ?, ?)",
		p.UserID,
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
			"INSERT INTO upload_session_chunks (user_id, digest, chunk_index, chunk_digest, size, uploaded) VALUES (?, ?, ?, ?, ?, ?)",
			p.UserID,
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

	return uploadStatus(db, p.UserID, manifest.Digest)
}

func markUploadChunk(db *sql.DB, userID int64, digest string, index int64, physicalBytes int64) error {
	_, err := db.Exec(`
		UPDATE upload_sessions
		SET physical_pending_bytes = physical_pending_bytes + ?, updated_at = CURRENT_TIMESTAMP
		WHERE user_id = ? AND digest = ?
	`, physicalBytes, userID, digest)
	if err != nil {
		return err
	}
	result, err := db.Exec(`
		UPDATE upload_session_chunks
		SET uploaded = 1
		WHERE user_id = ? AND digest = ? AND chunk_index = ?
	`, userID, digest, index)
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

func finalizeUpload(db *sql.DB, p Principal, digest string) (StoreBlobResponse, error) {
	manifest, physicalPendingBytes, err := uploadSessionManifest(db, p.UserID, digest)
	if err != nil {
		return StoreBlobResponse{}, err
	}

	status, err := uploadStatus(db, p.UserID, digest)
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

	exists, err := createOrRestoreRef(db, p, manifest.Digest, manifest.Size)
	if err != nil {
		return StoreBlobResponse{}, err
	}
	if physicalPendingBytes > 0 {
		if err := addPendingBytes(db, p.UserID, physicalPendingBytes); err != nil {
			return StoreBlobResponse{}, err
		}
	}
	if _, err := db.Exec("DELETE FROM upload_session_chunks WHERE user_id = ? AND digest = ?", p.UserID, manifest.Digest); err != nil {
		return StoreBlobResponse{}, err
	}
	if _, err := db.Exec("DELETE FROM upload_sessions WHERE user_id = ? AND digest = ?", p.UserID, manifest.Digest); err != nil {
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

		exists, err := storeSimpleManifest(db, p, manifest, payloads)
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
	refs, err := userBlobRefs(db, p.UserID)
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

		status, err := createUploadSession(db, getPrincipal(r), manifest)
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
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/uploads/"), "/")
	if len(parts) == 0 || parts[0] == "" || !validDigest(parts[0]) {
		http.Error(w, "invalid upload digest", http.StatusBadRequest)
		return
	}
	digest := parts[0]

	if len(parts) == 1 && r.Method == http.MethodGet {
		status, err := uploadStatus(db, p.UserID, digest)
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
		out, err := finalizeUpload(db, p, digest)
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
			WHERE user_id = ? AND digest = ? AND chunk_index = ?
		`, p.UserID, digest, index).Scan(&chunk.Index, &chunk.Digest, &chunk.Size)
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
		if err := markUploadChunk(db, p.UserID, digest, index, physicalBytes); err != nil {
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

	digest := strings.TrimPrefix(r.URL.Path, "/blob/")
	if digest == "" || strings.Contains(digest, "/") || strings.Contains(digest, "..") {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	exists, err := userHasCleanBlob(db, p.UserID, digest)
	if err != nil {
		http.Error(w, "failed to check blob ref", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "blob not found", http.StatusNotFound)
		return
	}

	if r.Method == http.MethodDelete {
		released, err := releaseBlobRef(db, p.UserID, digest)
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
	dirty INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
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
	key_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	chunk_size INTEGER NOT NULL,
	physical_pending_bytes INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (user_id, digest),
	FOREIGN KEY(user_id) REFERENCES users(id),
	FOREIGN KEY(key_id) REFERENCES key_ids(id)
);
CREATE TABLE IF NOT EXISTS upload_session_chunks (
	user_id INTEGER NOT NULL,
	digest TEXT NOT NULL,
	chunk_index INTEGER NOT NULL,
	chunk_digest TEXT NOT NULL,
	size INTEGER NOT NULL,
	uploaded INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (user_id, digest, chunk_index),
	FOREIGN KEY(user_id, digest) REFERENCES upload_sessions(user_id, digest)
);
CREATE INDEX IF NOT EXISTS upload_session_chunks_digest ON upload_session_chunks(digest);
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
	if err := ensureColumn(db, "blob_refs", "dirty", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		log.Fatal(err)
	}
	if err := ensureColumn(db, "upload_sessions", "physical_pending_bytes", "INTEGER NOT NULL DEFAULT 0"); err != nil {
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
	mux.HandleFunc("/uploads", requirePrincipal(db, handleUploadsPost(db)))

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
