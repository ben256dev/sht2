#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
  start_shtd
}

teardown() {
  teardown_test_env
}

@test "shtd POST/GET roundtrip" {
  payload="hello sht"
  resp="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  [ "$body" = "$payload" ]
}

@test "shtd simple upload creates manifest chunks" {
  payload="abc"
  resp="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  manifest_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_manifests WHERE digest = '$digest'")"
  chunk_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_manifest_chunks WHERE digest = '$digest'")"
  file_count="$(find "$SHT_BLOB_DIR/chunks" -type f | wc -l | tr -d ' ')"

  [ "$manifest_count" = "1" ]
  [ "$chunk_count" = "1" ]
  [ "$file_count" = "1" ]
}

@test "shtd rejects simple upload above user cutoff" {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_simple_upload_bytes = 3 WHERE id = 1"

  code="$(printf 'abcd' | curl -sS -o "$TEST_TMPDIR/upload.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"

  [ "$code" = "413" ]
  grep -q 'simple upload too large' "$TEST_TMPDIR/upload.out"
}

@test "shtd resumable upload finalizes manifest chunks" {
  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  chunk_digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  manifest='{"digest":"'"$digest"'","size":11,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$chunk_digest"'","size":11}]}'

  status="$(printf '%s' "$manifest" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'Content-Type: application/json' -X POST http://sht/uploads --data-binary @-)"
  [[ "$status" == *'"missing":[0]'* ]]
  [[ "$status" == *'"complete":false'* ]]

  finalize_code="$(curl -sS -o "$TEST_TMPDIR/finalize.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST "http://sht/uploads/$digest/finalize")"
  [ "$finalize_code" = "409" ]

  chunk_resp="$(printf 'hello world' | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X PUT "http://sht/uploads/$digest/chunks/0" --data-binary @-)"
  [[ "$chunk_resp" == *'"index":0'* ]]
  [[ "$chunk_resp" == *'"exists":false'* ]]

  status="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/uploads/$digest")"
  [[ "$status" == *'"uploaded":[0]'* ]]
  [[ "$status" == *'"complete":true'* ]]

  finalized="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST "http://sht/uploads/$digest/finalize")"
  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  session_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM upload_sessions WHERE digest = '$digest'")"

  [[ "$finalized" == *'"digest":"'"$digest"'"'* ]]
  [ "$body" = "hello world" ]
  [ "$session_count" = "0" ]
}

@test "shtd resumable upload can stop and resume missing chunks" {
  digest="cMxgIkD9FLQ2_bWESSej109pZeK2BQHnYFn8rL96ReM"
  chunk0_digest="8_88I-M-FIMeFsO4py2aBub41bRuSWXIn-rvOorWQOg"
  chunk1_digest="k7L_1-EJTXmjQyly-mJMnsBMJKibCCG6qkN1lWHR730"
  manifest='{"digest":"'"$digest"'","size":8388612,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$chunk0_digest"'","size":8388608},{"index":1,"digest":"'"$chunk1_digest"'","size":4}]}'

  python3 - <<'PY' "$TEST_TMPDIR"
import pathlib, sys
p = pathlib.Path(sys.argv[1])
(p / "chunk0").write_bytes(b"A" * (8 * 1024 * 1024))
(p / "chunk1").write_bytes(b"tail")
(p / "whole").write_bytes((p / "chunk0").read_bytes() + (p / "chunk1").read_bytes())
PY

  status="$(printf '%s' "$manifest" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'Content-Type: application/json' -X POST http://sht/uploads --data-binary @-)"
  [[ "$status" == *'"missing":[0,1]'* ]]

  printf '%s' "$TEST_TMPDIR/chunk0" >/dev/null
  chunk_resp="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X PUT "http://sht/uploads/$digest/chunks/0" --data-binary @"$TEST_TMPDIR/chunk0")"
  [[ "$chunk_resp" == *'"index":0'* ]]

  status="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/uploads/$digest")"
  [[ "$status" == *'"uploaded":[0]'* ]]
  [[ "$status" == *'"missing":[1]'* ]]
  [[ "$status" == *'"complete":false'* ]]

  finalize_code="$(curl -sS -o "$TEST_TMPDIR/finalize.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST "http://sht/uploads/$digest/finalize")"
  [ "$finalize_code" = "409" ]

  chunk_resp="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X PUT "http://sht/uploads/$digest/chunks/1" --data-binary @"$TEST_TMPDIR/chunk1")"
  [[ "$chunk_resp" == *'"index":1'* ]]

  status="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/uploads/$digest")"
  [[ "$status" == *'"uploaded":[0,1]'* ]]
  [[ "$status" == *'"missing":[]'* ]]
  [[ "$status" == *'"complete":true'* ]]

  curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST "http://sht/uploads/$digest/finalize" >/dev/null
  curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest" > "$TEST_TMPDIR/downloaded"
  cmp "$TEST_TMPDIR/whole" "$TEST_TMPDIR/downloaded"
}

@test "shtd resumable chunk upload verifies digest" {
  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  chunk_digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  manifest='{"digest":"'"$digest"'","size":11,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$chunk_digest"'","size":11}]}'

  printf '%s' "$manifest" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'Content-Type: application/json' -X POST http://sht/uploads --data-binary @- >/dev/null
  code="$(printf 'wrong bytes' | curl -sS -o "$TEST_TMPDIR/chunk.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X PUT "http://sht/uploads/$digest/chunks/0" --data-binary @-)"

  [ "$code" = "400" ]
  grep -q 'chunk digest mismatch' "$TEST_TMPDIR/chunk.out"
}

@test "shtd HEAD on missing blob returns 404" {
  code="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -I "http://sht/blob/does-not-exist")"
  [ "$code" = "404" ]
}

@test "shtd rejects digest path containing slash" {
  code="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/bad/name")"
  [ "$code" = "400" ]
}

@test "shtd dedup marks second upload exists=true" {
  payload="same content"
  r1="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  r2="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"

  [[ "$r1" == *'"exists":false'* ]]
  [[ "$r2" == *'"exists":true'* ]]
}

@test "shtd grants access by user blob ref" {
  payload="per-key blob"
  resp="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  code_ok="$(curl -sS -o /tmp/sht-k1.out -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  [ "$code_ok" = "200" ]

  code_other="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' "http://sht/blob/$digest")"
  [ "$code_other" = "404" ]
}

@test "shtd stores identical content once globally and creates refs per user" {
  payload="shared content"
  r1="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  r2="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$r1" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]
  [[ "$r1" == *'"exists":false'* ]]
  [[ "$r2" == *'"exists":false'* ]]

  ref_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_refs WHERE digest = '$digest'")"
  file_count="$(find "$SHT_BLOB_DIR" -type f | wc -l | tr -d ' ')"

  [ "$ref_count" = "2" ]
  [ "$file_count" = "1" ]
}

@test "shtd lists refs for authenticated user only" {
  r1="$(printf 'first' | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  r2="$(printf 'second' | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  r_other="$(printf 'other' | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' -X POST http://sht/blob --data-binary @-)"
  d1="$(printf '%s' "$r1" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"
  d2="$(printf '%s' "$r2" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"
  d_other="$(printf '%s' "$r_other" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"

  [[ "$refs" == *"$d1"* ]]
  [[ "$refs" == *"$d2"* ]]
  [[ "$refs" != *"$d_other"* ]]
  [[ "$refs" == *'"size":5'* ]]
  [[ "$refs" == *'"size":6'* ]]
  [[ "$refs" == *'"key_id":1'* ]]
  [[ "$refs" == *'"dirty":false'* ]]
}

@test "shtd released refs stay listed as dirty and lose access" {
  resp="$(printf 'gone' | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X DELETE "http://sht/blob/$digest"
  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  code="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"

  [[ "$refs" == *"$digest"* ]]
  [[ "$refs" == *'"dirty":true'* ]]
  [ "$code" = "404" ]
}

@test "shtd reupload restores dirty ref" {
  payload="restore me"
  resp="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X DELETE "http://sht/blob/$digest"

  resp2="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"

  [[ "$resp2" == *'"exists":false'* ]]
  [ "$body" = "$payload" ]
  [[ "$refs" == *"$digest"* ]]
  [[ "$refs" == *'"dirty":false'* ]]
}
