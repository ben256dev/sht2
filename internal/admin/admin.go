package admin

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

const DefaultDBPath = "/var/lib/sht/sht.db"

const DefaultUserMaxBytes int64 = 3 * 1024 * 1024 * 1024
const DefaultUserMaxPendingBytes int64 = DefaultUserMaxBytes + DefaultUserMaxBytes/4
const DefaultMaxSimpleUploadBytes int64 = 64 * 1024 * 1024
const DefaultShelfName = "main"

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type User struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	Enabled         bool   `json:"enabled"`
	ExternalSubject string `json:"external_subject,omitempty"`
	DisplayName     string `json:"display_name,omitempty"`
}

type Key struct {
	ID        int64  `json:"id"`
	UserName  string `json:"user"`
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	PublicKey string `json:"public_key"`
}

func OpenDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if err := EnsureSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
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

func EnsureSchema(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	enabled INTEGER NOT NULL DEFAULT 1,
	max_bytes INTEGER NOT NULL DEFAULT %d,
	max_pending_bytes INTEGER NOT NULL DEFAULT %d,
	pending_bytes INTEGER NOT NULL DEFAULT 0,
	max_simple_upload_bytes INTEGER NOT NULL DEFAULT %d,
	multi_shelf_enabled INTEGER NOT NULL DEFAULT 0,
	external_subject TEXT,
	display_name TEXT,
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
`, DefaultUserMaxBytes, DefaultUserMaxPendingBytes, DefaultMaxSimpleUploadBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", DefaultUserMaxBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_pending_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", DefaultUserMaxPendingBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "pending_bytes", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_simple_upload_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", DefaultMaxSimpleUploadBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "multi_shelf_enabled", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "external_subject", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "display_name", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "shelves", "is_default", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "key_ids", "public_key", "TEXT"); err != nil {
		return err
	}
	if _, err := db.Exec(`
CREATE UNIQUE INDEX IF NOT EXISTS users_external_subject_unique
ON users(external_subject)
WHERE external_subject IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS key_ids_public_key_unique
ON key_ids(public_key)
WHERE public_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS alias_grants_user_id ON alias_grants(user_id);
`); err != nil {
		return err
	}
	return nil
}

func ValidateIdentifier(kind, name string) error {
	if !identifierPattern.MatchString(name) {
		return fmt.Errorf("invalid %s: use 1-64 letters, numbers, dots, underscores, or hyphens", kind)
	}
	return nil
}

func ValidatePublicKey(publicKey string) error {
	publicKey = strings.TrimSpace(publicKey)
	if publicKey == "" {
		return fmt.Errorf("public key is empty")
	}
	prefixes := []string{
		"ssh-ed25519 ",
		"ssh-rsa ",
		"ecdsa-sha2-nistp256 ",
		"ecdsa-sha2-nistp384 ",
		"ecdsa-sha2-nistp521 ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(publicKey, prefix) {
			return nil
		}
	}
	return fmt.Errorf("unsupported public key type")
}

func UserIDByName(db interface {
	QueryRow(query string, args ...any) *sql.Row
}, name string) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT id FROM users WHERE name = ?", name).Scan(&id)
	return id, err
}

func userByExternalSubject(db *sql.DB, subject string) (User, error) {
	var u User
	var enabled int
	var externalSubject, displayName sql.NullString
	err := db.QueryRow(`
		SELECT id, name, enabled, external_subject, display_name
		FROM users
		WHERE external_subject = ?
	`, subject).Scan(&u.ID, &u.Name, &enabled, &externalSubject, &displayName)
	if err != nil {
		return User{}, err
	}
	u.Enabled = enabled == 1
	u.ExternalSubject = externalSubject.String
	u.DisplayName = displayName.String
	return u, nil
}

func CreateUser(db *sql.DB, name string) (User, error) {
	return createUser(db, name, "", "")
}

func CreateMappedUser(db *sql.DB, subject, username string) (User, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return User{}, fmt.Errorf("missing external subject")
	}
	if u, err := userByExternalSubject(db, subject); err == nil {
		return u, nil
	} else if err != sql.ErrNoRows {
		return User{}, err
	}

	base := safeUserName(username)
	if base == "" {
		base = "kc-" + shortSubjectHash(subject)
	}
	name := base
	if _, err := UserIDByName(db, name); err == nil {
		name = base + "-" + shortSubjectHash(subject)
	} else if err != sql.ErrNoRows {
		return User{}, err
	}
	return createUser(db, name, subject, username)
}

func createUser(db *sql.DB, name, externalSubject, displayName string) (User, error) {
	if err := ValidateIdentifier("user name", name); err != nil {
		return User{}, err
	}
	tx, err := db.Begin()
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback()

	var result sql.Result
	if externalSubject == "" {
		result, err = tx.Exec(`
INSERT INTO users (name, enabled, max_bytes, max_pending_bytes, pending_bytes, max_simple_upload_bytes, multi_shelf_enabled)
VALUES (?, 1, ?, ?, 0, ?, 0)
`, name, DefaultUserMaxBytes, DefaultUserMaxPendingBytes, DefaultMaxSimpleUploadBytes)
	} else {
		result, err = tx.Exec(`
INSERT INTO users (name, enabled, max_bytes, max_pending_bytes, pending_bytes, max_simple_upload_bytes, multi_shelf_enabled, external_subject, display_name)
VALUES (?, 1, ?, ?, 0, ?, 0, ?, ?)
`, name, DefaultUserMaxBytes, DefaultUserMaxPendingBytes, DefaultMaxSimpleUploadBytes, externalSubject, displayName)
	}
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return User{}, fmt.Errorf("user already exists")
		}
		return User{}, err
	}
	userID, err := result.LastInsertId()
	if err != nil {
		return User{}, err
	}
	if _, err := tx.Exec(`
INSERT INTO shelves (user_id, name, enabled, is_default, max_bytes, max_pending_bytes, pending_bytes)
VALUES (?, ?, 1, 1, ?, ?, 0)
`, userID, DefaultShelfName, DefaultUserMaxBytes, DefaultUserMaxPendingBytes); err != nil {
		return User{}, err
	}
	nsResult, err := tx.Exec("INSERT INTO alias_namespaces (name, owner_user_id) VALUES (?, ?)", name, userID)
	if err != nil {
		return User{}, err
	}
	namespaceID, err := nsResult.LastInsertId()
	if err != nil {
		return User{}, err
	}
	if _, err := tx.Exec("INSERT INTO alias_grants (namespace_id, path, user_id, role) VALUES (?, '', ?, 'admin')", namespaceID, userID); err != nil {
		return User{}, err
	}
	if err := tx.Commit(); err != nil {
		return User{}, err
	}
	return User{ID: userID, Name: name, Enabled: true, ExternalSubject: externalSubject, DisplayName: displayName}, nil
}

func safeUserName(username string) string {
	username = strings.TrimSpace(username)
	if username == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range username {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
		if b.Len() >= 64 {
			break
		}
	}
	name := strings.Trim(b.String(), ".-_")
	if ValidateIdentifier("user name", name) != nil {
		return ""
	}
	return name
}

func shortSubjectHash(subject string) string {
	sum := sha256.Sum256([]byte(subject))
	return hex.EncodeToString(sum[:])[:10]
}

func ListUsers(db *sql.DB) ([]User, error) {
	rows, err := db.Query("SELECT id, name, enabled, COALESCE(external_subject, ''), COALESCE(display_name, '') FROM users ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		var enabled int
		if err := rows.Scan(&u.ID, &u.Name, &enabled, &u.ExternalSubject, &u.DisplayName); err != nil {
			return nil, err
		}
		u.Enabled = enabled == 1
		users = append(users, u)
	}
	return users, rows.Err()
}

func DisableUser(db *sql.DB, name string) error {
	result, err := db.Exec("UPDATE users SET enabled = 0 WHERE name = ?", name)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("user not found")
	}
	return nil
}

func AddKeyForUserName(db *sql.DB, userName, keyName, publicKey string) (Key, error) {
	userID, err := UserIDByName(db, userName)
	if err == sql.ErrNoRows {
		return Key{}, fmt.Errorf("user not found")
	}
	if err != nil {
		return Key{}, err
	}
	return AddKeyForUserID(db, userID, userName, keyName, publicKey)
}

func AddKeyForUserID(db *sql.DB, userID int64, userName, keyName, publicKey string) (Key, error) {
	if err := ValidateIdentifier("key name", keyName); err != nil {
		return Key{}, err
	}
	publicKey = strings.TrimSpace(publicKey)
	if err := ValidatePublicKey(publicKey); err != nil {
		return Key{}, err
	}
	result, err := db.Exec("INSERT INTO key_ids (user_id, name, public_key, enabled) VALUES (?, ?, ?, 1)", userID, keyName, publicKey)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return Key{}, fmt.Errorf("public key already exists")
		}
		return Key{}, err
	}
	keyID, err := result.LastInsertId()
	if err != nil {
		return Key{}, err
	}
	return Key{ID: keyID, UserName: userName, Name: keyName, Enabled: true, PublicKey: publicKey}, nil
}

func AddKeyFromFile(db *sql.DB, userName, keyName, publicKeyPath string) (Key, error) {
	publicKeyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return Key{}, err
	}
	return AddKeyForUserName(db, userName, keyName, string(publicKeyBytes))
}

func ListKeys(db *sql.DB, userName string) ([]Key, error) {
	var rows *sql.Rows
	var err error
	if userName == "" {
		rows, err = db.Query(`
			SELECT key_ids.id, users.name, key_ids.name, key_ids.enabled, COALESCE(key_ids.public_key, '')
			FROM key_ids
			JOIN users ON users.id = key_ids.user_id
			ORDER BY key_ids.id
		`)
	} else {
		rows, err = db.Query(`
			SELECT key_ids.id, users.name, key_ids.name, key_ids.enabled, COALESCE(key_ids.public_key, '')
			FROM key_ids
			JOIN users ON users.id = key_ids.user_id
			WHERE users.name = ?
			ORDER BY key_ids.id
		`, userName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []Key
	for rows.Next() {
		var key Key
		var enabled int
		if err := rows.Scan(&key.ID, &key.UserName, &key.Name, &enabled, &key.PublicKey); err != nil {
			return nil, err
		}
		key.Enabled = enabled == 1
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func ListKeysForUserID(db *sql.DB, userID int64) ([]Key, error) {
	rows, err := db.Query(`
		SELECT key_ids.id, users.name, key_ids.name, key_ids.enabled, COALESCE(key_ids.public_key, '')
		FROM key_ids
		JOIN users ON users.id = key_ids.user_id
		WHERE users.id = ?
		ORDER BY key_ids.id
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []Key
	for rows.Next() {
		var key Key
		var enabled int
		if err := rows.Scan(&key.ID, &key.UserName, &key.Name, &enabled, &key.PublicKey); err != nil {
			return nil, err
		}
		key.Enabled = enabled == 1
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func DisableKey(db *sql.DB, keyIDText string) (int64, error) {
	keyID, err := strconv.ParseInt(keyIDText, 10, 64)
	if err != nil || keyID <= 0 {
		return 0, fmt.Errorf("invalid key id")
	}
	result, err := db.Exec("UPDATE key_ids SET enabled = 0 WHERE id = ?", keyID)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, fmt.Errorf("key not found")
	}
	return keyID, nil
}

func RenderAuthorizedKeys(db *sql.DB) (string, error) {
	rows, err := db.Query(`
		SELECT key_ids.id, key_ids.public_key
		FROM key_ids
		JOIN users ON users.id = key_ids.user_id
		WHERE key_ids.enabled = 1
		  AND users.enabled = 1
		  AND key_ids.public_key IS NOT NULL
		ORDER BY key_ids.id
	`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var b strings.Builder
	for rows.Next() {
		var id int64
		var publicKey string
		if err := rows.Scan(&id, &publicKey); err != nil {
			return "", err
		}
		publicKey = strings.TrimSpace(publicKey)
		if publicKey == "" {
			continue
		}
		fmt.Fprintf(&b, "command=\"sht-shell id %d\",restrict %s\n", id, publicKey)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	return b.String(), nil
}

func WriteFileAtomic(path string, data string, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	tmp, err := os.CreateTemp(dir, "."+base+".tmp.")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.WriteString(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func SyncAuthorizedKeys(db *sql.DB, output string) error {
	authorizedKeys, err := RenderAuthorizedKeys(db)
	if err != nil {
		return err
	}
	return WriteFileAtomic(output, authorizedKeys, 0600)
}
