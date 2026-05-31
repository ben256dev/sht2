#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

set_limits() {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_bytes = $2, max_pending_bytes = $3, pending_bytes = 0 WHERE id = $1"
}

upload() {
  local key_id="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H "X-SHT-Key-ID: $key_id" -X POST http://sht/blob --data-binary @-
}

upload_code() {
  local key_id="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS -o "$TEST_TMPDIR/upload.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H "X-SHT-Key-ID: $key_id" -X POST http://sht/blob --data-binary @-
}

release_code() {
  local key_id="$1"
  local digest="$2"
  curl -sS -o "$TEST_TMPDIR/release.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H "X-SHT-Key-ID: $key_id" -X DELETE "http://sht/blob/$digest"
}

digest_from_response() {
  sed -n 's/.*"digest":"\([^"]*\)".*/\1/p'
}

pending_bytes() {
  sqlite3 "$SHT_DB_PATH" "SELECT pending_bytes FROM users WHERE id = $1"
}

@test "shtd release removes user access" {
  start_shtd

  digest="$(upload 1 'release me' | digest_from_response)"
  [ -n "$digest" ]

  [ "$(release_code 1 "$digest")" = "204" ]

  code="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  [ "$code" = "404" ]
}

@test "shtd release frees live quota but not pending quota" {
  set_limits 1 3 10
  start_shtd

  digest="$(upload 1 'abc' | digest_from_response)"
  [ -n "$digest" ]
  [ "$(release_code 1 "$digest")" = "204" ]
  [ "$(upload_code 1 'def')" = "200" ]
  [ "$(pending_bytes 1)" = "6" ]
}

@test "shtd release does not free pending quota before manual gc" {
  set_limits 1 20 3
  start_shtd

  digest="$(upload 1 'abc' | digest_from_response)"
  [ -n "$digest" ]
  [ "$(release_code 1 "$digest")" = "204" ]

  [ "$(upload_code 1 'd')" = "413" ]
  grep -q 'pending quota exceeded' "$TEST_TMPDIR/upload.out"
}

@test "shtd manual gc reset allows new pending uploads" {
  set_limits 1 20 3
  start_shtd

  digest="$(upload 1 'abc' | digest_from_response)"
  [ -n "$digest" ]
  [ "$(release_code 1 "$digest")" = "204" ]
  [ "$(upload_code 1 'd')" = "413" ]

  find "$SHT_BLOB_DIR" -type f -delete
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET pending_bytes = 0"

  [ "$(upload_code 1 'd')" = "200" ]
}

@test "shtd existing global blob does not charge pending quota" {
  set_limits 2 20 2
  start_shtd

  [ "$(upload_code 1 'abc')" = "200" ]
  [ "$(upload_code 2 'abc')" = "200" ]
  [ "$(pending_bytes 2)" = "0" ]
}

@test "shtd allows released live quota to be substituted while pending quota has headroom" {
  set_limits 1 3 4
  start_shtd

  d1="$(upload 1 '1' | digest_from_response)"
  d2="$(upload 1 '2' | digest_from_response)"
  d3="$(upload 1 '3' | digest_from_response)"
  [ -n "$d1" ]
  [ -n "$d2" ]
  [ -n "$d3" ]

  [ "$(upload_code 1 '4')" = "413" ]
  grep -q 'quota exceeded' "$TEST_TMPDIR/upload.out"

  [ "$(release_code 1 "$d3")" = "204" ]

  [ "$(upload_code 1 '4')" = "200" ]
  d4="$(cat "$TEST_TMPDIR/upload.out" | digest_from_response)"
  [ -n "$d4" ]
  [ "$(pending_bytes 1)" = "4" ]

  [ "$(release_code 1 "$d4")" = "204" ]

  [ "$(upload_code 1 '5')" = "413" ]
  grep -q 'pending quota exceeded' "$TEST_TMPDIR/upload.out"
}
