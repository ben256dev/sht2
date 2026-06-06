#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

@test "shtd migrates key_ids to store SSH public keys" {
  start_shtd

  columns="$(sqlite3 "$SHT_DB_PATH" "PRAGMA table_info(key_ids);")"
  [[ "$columns" == *"|public_key|TEXT|"* ]]

  sqlite3 "$SHT_DB_PATH" \
    "INSERT INTO key_ids (id, user_id, name, public_key, enabled) VALUES (10, 1, 'laptop', 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILaptopKey test@laptop', 1);"

  stored="$(sqlite3 "$SHT_DB_PATH" "SELECT public_key FROM key_ids WHERE id = 10;")"
  [ "$stored" = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILaptopKey test@laptop" ]
}

@test "shtd enforces unique SSH public keys" {
  start_shtd

  public_key='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAISharedKey shared@example'
  sqlite3 "$SHT_DB_PATH" \
    "INSERT INTO key_ids (id, user_id, name, public_key, enabled) VALUES (10, 1, 'first', '$public_key', 1);"

  run sqlite3 "$SHT_DB_PATH" \
    "INSERT INTO key_ids (id, user_id, name, public_key, enabled) VALUES (11, 2, 'second', '$public_key', 1);"
  [ "$status" -ne 0 ]
  [[ "$output" == *"UNIQUE"* || "$output" == *"constraint"* ]]
}

@test "shtd allows legacy key IDs to remain without public keys during migration" {
  start_shtd

  count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM key_ids WHERE public_key IS NULL;")"
  [ "$count" = "3" ]

  code="$(printf 'legacy key still authenticates' | curl -sS -o "$TEST_TMPDIR/upload.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "200" ]
}

@test "shtd migrates legacy external subjects to external keycloak users" {
  sqlite3 "$SHT_DB_PATH" "ALTER TABLE users ADD COLUMN external_subject TEXT; UPDATE users SET external_subject = 'kc-legacy-subject' WHERE id = 1;"

  start_shtd

  row="$(sqlite3 "$SHT_DB_PATH" "SELECT kind, identity_provider FROM users WHERE id = 1;")"
  [ "$row" = "external|keycloak" ]
}
