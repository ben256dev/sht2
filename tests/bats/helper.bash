#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_SHTD="$ROOT_DIR/bin/shtd"
BIN_SHELL="$ROOT_DIR/bin/sht-shell"

setup_test_env() {
  TEST_TMPDIR="$(mktemp -d)"
  export TEST_TMPDIR
  export SHT_SOCK_DIR="$TEST_TMPDIR/sht.sock"
  export SHT_BLOB_DIR="$TEST_TMPDIR/blobs"
  export SHT_TMP_DIR="$TEST_TMPDIR/tmp"
  export SHT_DB_PATH="$TEST_TMPDIR/sht.db"
  unset SHT_SOCK_MODE
  unset SHT_SOCK_GROUP
  seed_test_db
}

seed_test_db() {
  sqlite3 "$SHT_DB_PATH" <<'SQL'
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  enabled INTEGER NOT NULL DEFAULT 1,
  max_bytes INTEGER NOT NULL DEFAULT 3221225472,
  max_pending_bytes INTEGER NOT NULL DEFAULT 4026531840,
  pending_bytes INTEGER NOT NULL DEFAULT 0,
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
INSERT OR REPLACE INTO users (id, name, enabled, max_bytes, max_pending_bytes, pending_bytes) VALUES (1, 'test-user-1', 1, 3221225472, 4026531840, 0);
INSERT OR REPLACE INTO users (id, name, enabled, max_bytes, max_pending_bytes, pending_bytes) VALUES (2, 'test-user-2', 1, 3221225472, 4026531840, 0);
INSERT OR REPLACE INTO key_ids (id, user_id, name, enabled) VALUES (1, 1, 'test-key-1', 1);
INSERT OR REPLACE INTO key_ids (id, user_id, name, enabled) VALUES (2, 2, 'test-key-2', 1);
INSERT OR REPLACE INTO key_ids (id, user_id, name, enabled) VALUES (3, 1, 'test-key-3', 1);
SQL
}

teardown_test_env() {
  if [[ -n "${SHTD_PID:-}" ]]; then
    kill "$SHTD_PID" >/dev/null 2>&1 || true
    wait "$SHTD_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "${TEST_TMPDIR:-}"
}

start_shtd() {
  "$BIN_SHTD" >"$TEST_TMPDIR/shtd.log" 2>&1 &
  SHTD_PID=$!
  export SHTD_PID

  for _ in $(seq 1 50); do
    [[ -S "$SHT_SOCK_DIR" ]] && return 0
    sleep 0.05
  done

  echo "shtd failed to start" >&2
  [[ -f "$TEST_TMPDIR/shtd.log" ]] && cat "$TEST_TMPDIR/shtd.log" >&2
  return 1
}
