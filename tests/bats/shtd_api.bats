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

@test "shtd isolates blobs by key id" {
  payload="per-key blob"
  resp="$(printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/blob --data-binary @-)"
  digest="$(printf '%s' "$resp" | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p')"

  [ -n "$digest" ]

  code_ok="$(curl -sS -o /tmp/sht-k1.out -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  [ "$code_ok" = "200" ]

  code_other="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' "http://sht/blob/$digest")"
  [ "$code_other" = "404" ]
}
