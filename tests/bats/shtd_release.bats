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

blob_file() {
  local digest="$1"
  if [ "${#digest}" -lt 3 ]; then
    printf '%s/%s\n' "$SHT_BLOB_DIR" "$digest"
    return
  fi
  printf '%s/%s/%s\n' "$SHT_BLOB_DIR" "${digest:0:2}" "${digest:2}"
}

manual_gc() {
  local digest
  while IFS= read -r digest; do
    [ -n "$digest" ] || continue
    rm -f "$(blob_file "$digest")"
  done < <(sqlite3 "$SHT_DB_PATH" "
    SELECT DISTINCT dirty_refs.digest
    FROM blob_refs AS dirty_refs
    WHERE dirty_refs.dirty = 1
      AND NOT EXISTS (
        SELECT 1
        FROM blob_refs AS clean_refs
        WHERE clean_refs.digest = dirty_refs.digest
          AND clean_refs.dirty = 0
      );
  ")

  sqlite3 "$SHT_DB_PATH" "DELETE FROM blob_refs WHERE dirty = 1; UPDATE users SET pending_bytes = 0"
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

  manual_gc

  [ "$(upload_code 1 'd')" = "200" ]
}

@test "shtd manual gc removes dirty refs" {
  set_limits 1 20 3
  start_shtd

  digest="$(upload 1 'abc' | digest_from_response)"
  [ -n "$digest" ]
  [ "$(release_code 1 "$digest")" = "204" ]

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  [[ "$refs" == *"$digest"* ]]
  [[ "$refs" == *'"dirty":true'* ]]

  manual_gc

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  [[ "$refs" != *"$digest"* ]]

  [ ! -e "$(blob_file "$digest")" ]
}

@test "shtd manual gc preserves blobs still referenced by another user" {
  set_limits 1 20 10
  set_limits 2 20 10
  start_shtd

  payload="shared"
  digest="$(upload 1 "$payload" | digest_from_response)"
  [ -n "$digest" ]
  [ "$(upload_code 2 "$payload")" = "200" ]
  [ "$(release_code 1 "$digest")" = "204" ]

  manual_gc

  refs_user1="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  body_user2="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' "http://sht/blob/$digest")"

  [[ "$refs_user1" != *"$digest"* ]]
  [ "$body_user2" = "$payload" ]
  [ -e "$(blob_file "$digest")" ]
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
