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

create_manifest_session() {
  local digest="$1"
  local size="$2"
  local manifest='{"digest":"'"$digest"'","size":'"$size"',"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$digest"'","size":'"$size"'}]}'
  printf '%s' "$manifest" | curl -sS -o "$TEST_TMPDIR/manifest.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'Content-Type: application/json' -X POST http://sht/uploads --data-binary @-
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

  quota="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/quota)"
  [[ "$quota" == *'"used_bytes":3'* ]]
  [[ "$quota" == *'"max_bytes":3'* ]]
  [[ "$quota" == *'"pending_bytes":3'* ]]
  [[ "$quota" == *'"clean_digest_count":1'* ]]
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

@test "shtd quota includes reserved resumable upload bytes and enforces them" {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_bytes = 10, max_pending_bytes = 20 WHERE id = 1"
  start_shtd

  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  [ "$(create_manifest_session "$digest" 10)" = "200" ]

  quota="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/quota)"
  [[ "$quota" == *'"used_bytes":0'* ]]
  [[ "$quota" == *'"pending_bytes":0'* ]]
  [[ "$quota" == *'"upload_reserved_bytes":10'* ]]

  [ "$(upload_code 1 'x')" = "413" ]
  grep -q 'quota exceeded' "$TEST_TMPDIR/upload.out"
  grep -q 'reserved 10 bytes' "$TEST_TMPDIR/upload.out"
}
