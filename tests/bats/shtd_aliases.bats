#!/usr/bin/env bats

load helper

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

upload_blob() {
  local key_id="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H "X-SHT-Key-ID: $key_id" \
    -X POST http://sht/blob \
    --data-binary @- |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])'
}

alias_set() {
  local key_id="$1"
  local namespace="$2"
  local path="$3"
  local digest="$4"
  local expected="${5:-}"
  local body
  if [[ -n "$expected" ]]; then
    body='{"digest":"'"$digest"'","expected_version":'"$expected"'}'
  else
    body='{"digest":"'"$digest"'"}'
  fi
  printf '%s' "$body" | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H "X-SHT-Key-ID: $key_id" \
    -H 'Content-Type: application/json' \
    -X POST "http://sht/alias/$namespace/$path/versions" \
    --data-binary @-
}

@test "shtd aliases create versioned names and detect stale writes" {
  start_shtd

  namespaces="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/alias/namespaces)"
  [[ "$namespaces" == *"test-user-1"* ]]

  created="$(printf '{"name":"team"}' | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/namespaces \
    --data-binary @-)"
  [[ "$created" == *'"name":"team"'* ]]

  d1="$(upload_blob 1 abc)"
  v1="$(alias_set 1 team docs/readme "$d1")"
  id1="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$v1")"
  [ "$id1" -gt 0 ]

  alias_json="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/alias/team/docs/readme)"
  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/alias/team/docs/readme/blob)"
  list_json="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' 'http://sht/alias/team?prefix=docs')"

  d2="$(upload_blob 1 def)"
  v2="$(alias_set 1 team docs/readme "$d2" "$id1")"
  id2="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$v2")"

  code="$(printf '{"digest":"'"$d1"'","expected_version":'"$id1"'}' | curl -sS -o "$TEST_TMPDIR/conflict.out" -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/team/docs/readme/versions \
    --data-binary @-)"
  history="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/alias/team/docs/readme/versions)"

  python3 - "$alias_json" "$list_json" "$history" "$d1" "$d2" "$id1" "$id2" "$code" "$TEST_TMPDIR/conflict.out" <<'PY'
import json
import sys

alias_json, list_json, history = [json.loads(arg) for arg in sys.argv[1:4]]
d1, d2 = sys.argv[4:6]
id1, id2 = map(int, sys.argv[6:8])
code, conflict_path = sys.argv[8:10]
conflict = json.load(open(conflict_path))

assert alias_json["digest"] == d1
assert alias_json["current_version_id"] == id1
assert list_json["aliases"][0]["path"] == "docs/readme"
assert code == "409"
assert conflict["current_version"] == id2
assert [v["digest"] for v in history["versions"]] == [d2, d1]
assert [v["id"] for v in history["versions"]] == [id2, id1]
PY
  [ "$body" = "abc" ]
}

@test "shtd alias grants apply recursively and require write for updates" {
  start_shtd

  printf '{"name":"shared"}' | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/namespaces \
    --data-binary @- >/dev/null
  d1="$(upload_blob 1 alpha)"
  v1="$(alias_set 1 shared docs/file "$d1")"
  id1="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$v1")"

  code="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' http://sht/alias/shared/docs/file)"
  [ "$code" = "403" ]

  printf '{"path":"docs","user":"test-user-2","role":"read"}' | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/shared/grants \
    --data-binary @- >/dev/null

  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 2' http://sht/alias/shared/docs/file/blob)"
  d2="$(upload_blob 2 beta)"
  code="$(printf '{"digest":"'"$d2"'","expected_version":'"$id1"'}' | curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 2' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/shared/docs/file/versions \
    --data-binary @-)"
  [ "$code" = "403" ]

  printf '{"path":"docs","user":"test-user-2","role":"write"}' | curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/shared/grants \
    --data-binary @- >/dev/null
  v2="$(alias_set 2 shared docs/file "$d2" "$id1")"

  code="$(printf '{"path":"docs","user":"test-user-1","role":"read"}' | curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 2' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/alias/shared/grants \
    --data-binary @-)"

  python3 - "$v2" "$d2" <<'PY'
import json
import sys

version = json.loads(sys.argv[1])
assert version["digest"] == sys.argv[2]
assert version["author_user"] == "test-user-2"
PY
  [ "$body" = "alpha" ]
  [ "$code" = "403" ]
}
