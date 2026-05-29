#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

set_user_limit() {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_bytes = $2 WHERE id = $1"
}

upload_code() {
  local key_id="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS -o "$TEST_TMPDIR/upload.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H "X-SHT-Key-ID: $key_id" -X POST http://sht/blob --data-binary @-
}

@test "shtd rejects new upload when user quota would be exceeded" {
  set_user_limit 1 10
  start_shtd

  [ "$(upload_code 1 '1234567890')" = "200" ]
  [ "$(upload_code 1 'x')" = "413" ]
  grep -q 'quota exceeded' "$TEST_TMPDIR/upload.out"
}

@test "shtd allows duplicate upload when user is at quota" {
  set_user_limit 1 3
  start_shtd

  [ "$(upload_code 1 'abc')" = "200" ]
  [ "$(upload_code 1 'abc')" = "200" ]
  grep -q '"exists":true' "$TEST_TMPDIR/upload.out"
}

@test "shtd applies quota across keys for the same user" {
  set_user_limit 1 5
  start_shtd

  [ "$(upload_code 1 'abc')" = "200" ]
  [ "$(upload_code 3 'def')" = "413" ]
}

@test "shtd charges quota before referencing globally stored blob" {
  set_user_limit 2 2
  start_shtd

  [ "$(upload_code 1 'abc')" = "200" ]
  [ "$(upload_code 2 'abc')" = "413" ]

  ref_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_refs")"
  file_count="$(find "$SHT_BLOB_DIR" -type f | wc -l | tr -d ' ')"

  [ "$ref_count" = "1" ]
  [ "$file_count" = "1" ]
}
