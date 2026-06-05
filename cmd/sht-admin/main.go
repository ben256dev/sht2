package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

var dbPath = getenv("SHT_DB_PATH", "/var/lib/sht/sht.db")

const defaultUserMaxBytes int64 = 3 * 1024 * 1024 * 1024
const defaultUserMaxPendingBytes int64 = defaultUserMaxBytes + defaultUserMaxBytes/4
const defaultMaxSimpleUploadBytes int64 = 64 * 1024 * 1024
const defaultShelfName = "main"

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func usage() string {
	return strings.TrimSpace(`
usage:
  sht-admin user create <name>
  sht-admin user list
  sht-admin user disable <name>
  sht-admin key add <user> <name> <public-key-file>
  sht-admin key list [user]
  sht-admin key disable <key-id>
  sht-admin authorized-keys sync --output <path>
`)
}

func userUsage() string {
	return strings.TrimSpace(`
usage:
  sht-admin user create <name>
  sht-admin user list
  sht-admin user disable <name>
`)
}

func keyUsage() string {
	return strings.TrimSpace(`
usage:
  sht-admin key add <user> <name> <public-key-file>
  sht-admin key list [user]
  sht-admin key disable <key-id>
`)
}

func authorizedKeysUsage() string {
	return "usage: sht-admin authorized-keys sync --output <path>"
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func openDB(path string) *sql.DB {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		die("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		die("open db: %v", err)
	}
	if err := ensureSchema(db); err != nil {
		die("migrate db: %v", err)
	}
	return db
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

func ensureSchema(db *sql.DB) error {
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
`, defaultUserMaxBytes, defaultUserMaxPendingBytes, defaultMaxSimpleUploadBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultUserMaxBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_pending_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultUserMaxPendingBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "pending_bytes", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "max_simple_upload_bytes", fmt.Sprintf("INTEGER NOT NULL DEFAULT %d", defaultMaxSimpleUploadBytes)); err != nil {
		return err
	}
	if err := ensureColumn(db, "users", "multi_shelf_enabled", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "shelves", "is_default", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "key_ids", "public_key", "TEXT"); err != nil {
		return err
	}
	if _, err := db.Exec(`
CREATE UNIQUE INDEX IF NOT EXISTS key_ids_public_key_unique
ON key_ids(public_key)
WHERE public_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS alias_grants_user_id ON alias_grants(user_id);
`); err != nil {
		return err
	}
	return nil
}

func validateIdentifier(kind, name string) error {
	if !identifierPattern.MatchString(name) {
		return fmt.Errorf("invalid %s: use 1-64 letters, numbers, dots, underscores, or hyphens", kind)
	}
	return nil
}

func validatePublicKey(publicKey string) error {
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

func userIDByName(tx interface {
	QueryRow(query string, args ...any) *sql.Row
}, name string) (int64, error) {
	var id int64
	err := tx.QueryRow("SELECT id FROM users WHERE name = ?", name).Scan(&id)
	return id, err
}

func createUser(db *sql.DB, name string) error {
	if err := validateIdentifier("user name", name); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.Exec(`
INSERT INTO users (name, enabled, max_bytes, max_pending_bytes, pending_bytes, max_simple_upload_bytes, multi_shelf_enabled)
VALUES (?, 1, ?, ?, 0, ?, 0)
`, name, defaultUserMaxBytes, defaultUserMaxPendingBytes, defaultMaxSimpleUploadBytes)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("user already exists")
		}
		return err
	}
	userID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`
INSERT INTO shelves (user_id, name, enabled, is_default, max_bytes, max_pending_bytes, pending_bytes)
VALUES (?, ?, 1, 1, ?, ?, 0)
`, userID, defaultShelfName, defaultUserMaxBytes, defaultUserMaxPendingBytes); err != nil {
		return err
	}
	nsResult, err := tx.Exec("INSERT INTO alias_namespaces (name, owner_user_id) VALUES (?, ?)", name, userID)
	if err != nil {
		return err
	}
	namespaceID, err := nsResult.LastInsertId()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO alias_grants (namespace_id, path, user_id, role) VALUES (?, '', ?, 'admin')", namespaceID, userID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Printf("%d %s enabled\n", userID, name)
	return nil
}

func listUsers(db *sql.DB) error {
	rows, err := db.Query("SELECT id, name, enabled FROM users ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		var enabled int
		if err := rows.Scan(&id, &name, &enabled); err != nil {
			return err
		}
		state := "disabled"
		if enabled == 1 {
			state = "enabled"
		}
		fmt.Printf("%d %s %s\n", id, name, state)
	}
	return rows.Err()
}

func disableUser(db *sql.DB, name string) error {
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
	fmt.Printf("%s disabled\n", name)
	return nil
}

func addKey(db *sql.DB, userName, keyName, publicKeyPath string) error {
	if err := validateIdentifier("key name", keyName); err != nil {
		return err
	}
	publicKeyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return err
	}
	publicKey := strings.TrimSpace(string(publicKeyBytes))
	if err := validatePublicKey(publicKey); err != nil {
		return err
	}
	userID, err := userIDByName(db, userName)
	if err == sql.ErrNoRows {
		return fmt.Errorf("user not found")
	}
	if err != nil {
		return err
	}
	result, err := db.Exec("INSERT INTO key_ids (user_id, name, public_key, enabled) VALUES (?, ?, ?, 1)", userID, keyName, publicKey)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("public key already exists")
		}
		return err
	}
	keyID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	fmt.Printf("%d %s %s enabled\n", keyID, userName, keyName)
	return nil
}

func listKeys(db *sql.DB, args []string) error {
	var rows *sql.Rows
	var err error
	if len(args) == 0 {
		rows, err = db.Query(`
			SELECT key_ids.id, users.name, key_ids.name, key_ids.enabled, COALESCE(key_ids.public_key, '')
			FROM key_ids
			JOIN users ON users.id = key_ids.user_id
			ORDER BY key_ids.id
		`)
	} else if len(args) == 1 {
		rows, err = db.Query(`
			SELECT key_ids.id, users.name, key_ids.name, key_ids.enabled, COALESCE(key_ids.public_key, '')
			FROM key_ids
			JOIN users ON users.id = key_ids.user_id
			WHERE users.name = ?
			ORDER BY key_ids.id
		`, args[0])
	} else {
		return fmt.Errorf("usage: sht-admin key list [user]")
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var userName, keyName, publicKey string
		var enabled int
		if err := rows.Scan(&id, &userName, &keyName, &enabled, &publicKey); err != nil {
			return err
		}
		state := "disabled"
		if enabled == 1 {
			state = "enabled"
		}
		fmt.Printf("%d %s %s %s %s\n", id, userName, keyName, state, publicKey)
	}
	return rows.Err()
}

func disableKey(db *sql.DB, keyIDText string) error {
	keyID, err := strconv.ParseInt(keyIDText, 10, 64)
	if err != nil || keyID <= 0 {
		return fmt.Errorf("invalid key id")
	}
	result, err := db.Exec("UPDATE key_ids SET enabled = 0 WHERE id = ?", keyID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("key not found")
	}
	fmt.Printf("%d disabled\n", keyID)
	return nil
}

func parseAuthorizedKeysSyncArgs(args []string) string {
	var output string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			fmt.Println(authorizedKeysUsage())
			os.Exit(0)
		case "--output":
			if i+1 >= len(args) {
				die("%s", authorizedKeysUsage())
			}
			output = args[i+1]
			i++
		default:
			die("unknown authorized-keys sync option: %s\n%s", args[i], authorizedKeysUsage())
		}
	}
	if output == "" {
		die("%s", authorizedKeysUsage())
	}
	return output
}

func renderAuthorizedKeys(db *sql.DB) (string, error) {
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

func writeFileAtomic(path string, data string, mode fs.FileMode) error {
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

func syncAuthorizedKeys(output string) error {
	db := openDB(dbPath)
	defer db.Close()

	authorizedKeys, err := renderAuthorizedKeys(db)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(output, authorizedKeys, 0600); err != nil {
		return err
	}
	return nil
}

func userCommand(db *sql.DB, args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(userUsage())
		if len(args) == 0 {
			return fmt.Errorf("missing user command")
		}
		return nil
	}
	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin user create <name>")
		}
		return createUser(db, args[1])
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: sht-admin user list")
		}
		return listUsers(db)
	case "disable":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin user disable <name>")
		}
		return disableUser(db, args[1])
	default:
		return fmt.Errorf("unknown user command: %s\n%s", args[0], userUsage())
	}
}

func keyCommand(db *sql.DB, args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(keyUsage())
		if len(args) == 0 {
			return fmt.Errorf("missing key command")
		}
		return nil
	}
	switch args[0] {
	case "add":
		if len(args) != 4 {
			return fmt.Errorf("usage: sht-admin key add <user> <name> <public-key-file>")
		}
		return addKey(db, args[1], args[2], args[3])
	case "list":
		return listKeys(db, args[1:])
	case "disable":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin key disable <key-id>")
		}
		return disableKey(db, args[1])
	default:
		return fmt.Errorf("unknown key command: %s\n%s", args[0], keyUsage())
	}
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		die("%s", usage())
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(usage())
		return
	}

	if len(args) >= 2 && args[0] == "authorized-keys" && args[1] == "sync" {
		output := parseAuthorizedKeysSyncArgs(args[2:])
		if err := syncAuthorizedKeys(output); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				die("authorized-keys sync failed: %v", err)
			}
			die("authorized-keys sync failed: %v", err)
		}
		return
	}

	db := openDB(dbPath)
	defer db.Close()

	var err error
	switch args[0] {
	case "user":
		err = userCommand(db, args[1:])
	case "key":
		err = keyCommand(db, args[1:])
	case "authorized-keys":
		err = fmt.Errorf("unknown authorized-keys command\n%s", authorizedKeysUsage())
	default:
		err = fmt.Errorf("%s", usage())
	}
	if err != nil {
		die("%v", err)
	}
}
