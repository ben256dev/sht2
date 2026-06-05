#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

@test "sht-shell fails malformed invocation" {
  run "$BIN_SHELL"
  [ "$status" -ne 0 ]
  [[ "$output" == *"malformed command"* ]]
}

@test "sht-shell rejects invalid key id" {
  run "$BIN_SHELL" id nope
  [ "$status" -ne 0 ]
  [[ "$output" == *"invalid key id"* ]]
}

@test "sht-shell help prints usage" {
  run env SSH_ORIGINAL_COMMAND='help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"usage:"* ]]
  [[ "$output" == *"sht --"* ]]
  [[ "$output" == *"cat [digest ...]"* ]]
  [[ "$output" == *"list [fields]"* ]]
  [[ "$output" == *"quota"* ]]
}

@test "sht-shell list help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='list -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [fields]"* ]]
  [[ "$output" == *"d  digest"* ]]
  [[ "$output" == *"h  help"* ]]
  [[ "$output" == *"t  state"* ]]
  [[ "$output" == *"sht list dt"* ]]

  run env SSH_ORIGINAL_COMMAND='list help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [fields]"* ]]

  run env SSH_ORIGINAL_COMMAND='list -help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [fields]"* ]]
}

@test "sht-shell scoped list help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='app1 list -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [fields]"* ]]
  [[ "$output" == *"sht <shelf> list dt"* ]]

  run env SSH_ORIGINAL_COMMAND='app1 list help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [fields]"* ]]
}

@test "sht-shell digest command help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='release -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht release [digest ...]"* ]]
  [[ "$output" == *"digests are read from stdin"* ]]
}

@test "sht-shell shelf help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='shelf -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht shelf list"* ]]
  [[ "$output" == *"sht shelf default <name>"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht shelf list"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf bogus' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"sht shelf list"* ]]
  [[ "$output" != *"list|create"* ]]
}

@test "sht-shell alias help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='alias help' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht alias ns list"* ]]
  [[ "$output" == *"sht alias set <namespace> <path> <digest>"* ]]

  run env SSH_ORIGINAL_COMMAND='alias bogus' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"sht alias ns list"* ]]
  [[ "$output" != *"ns|list|get"* ]]
}

@test "sht-shell json help prints structured usage" {
  run env SSH_ORIGINAL_COMMAND='help list -j' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["topic"] == "list"
assert "sht list [fields]" in out["usage"]
assert "d  digest" in out["usage"]
PY

  run env SSH_ORIGINAL_COMMAND='help alias -j' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["topic"] == "alias"
assert "sht alias ns list" in out["usage"]
assert "sht alias revoke" in out["usage"]
PY

  run env SSH_ORIGINAL_COMMAND='help nope -j' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert "unknown help topic" in out["error"]["message"]
PY
}

@test "sht-shell stat on missing blob returns not found" {
  start_shtd
  run env SSH_ORIGINAL_COMMAND='stat not-real' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell store+cat roundtrip" {
  start_shtd

  payload="shell roundtrip payload"
  store_out="$(printf '%s' "$payload" | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1)"
  digest="$(printf '%s\n' "$store_out" | tail -n1)"

  [ -n "$digest" ]

  cat_out="$(SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1)"
  cat_payload="$(printf '%s\n' "$cat_out" | tail -n1)"
  [ "$cat_payload" = "$payload" ]
}

@test "sht-shell cat reads multiple digests from stdin" {
  start_shtd

  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"

  [ -n "$d1" ]
  [ -n "$d2" ]

  cat_out="$(printf '%s\n%s\n' "$d1" "$d2" | SSH_ORIGINAL_COMMAND='cat' "$BIN_SHELL" id 1)"
  [ "$cat_out" = "onetwo" ]
}

@test "sht-shell stat handles multiple digest arguments" {
  start_shtd

  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"

  stat_out="$(SSH_ORIGINAL_COMMAND="stat $d1 $d2" "$BIN_SHELL" id 1)"

  [[ "$stat_out" == *"$d1 exists"* ]]
  [[ "$stat_out" == *"$d2 exists"* ]]
}

@test "sht-shell release removes blob access" {
  start_shtd

  payload="shell release payload"
  store_out="$(printf '%s' "$payload" | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1)"
  digest="$(printf '%s\n' "$store_out" | tail -n1)"

  [ -n "$digest" ]

  release_out="$(SSH_ORIGINAL_COMMAND="release $digest" "$BIN_SHELL" id 1)"
  [ "$release_out" = "released" ]

  run env SSH_ORIGINAL_COMMAND="stat $digest" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell release reads multiple digests from stdin" {
  start_shtd

  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"

  release_out="$(printf '%s %s' "$d1" "$d2" | SSH_ORIGINAL_COMMAND='release' "$BIN_SHELL" id 1)"

  [[ "$release_out" == *"$d1 released"* ]]
  [[ "$release_out" == *"$d2 released"* ]]

  run env SSH_ORIGINAL_COMMAND="stat $d1" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell list lists uploaded blob" {
  start_shtd

  payload="shell refs payload"
  store_out="$(printf '%s' "$payload" | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1)"
  digest="$(printf '%s\n' "$store_out" | tail -n1)"

  [ -n "$digest" ]

  refs_out="$(SSH_ORIGINAL_COMMAND='list' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest  main  18  1"* ]]
}

@test "sht-shell list can print selected fields" {
  start_shtd

  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"

  refs_out="$(SSH_ORIGINAL_COMMAND='list -d' "$BIN_SHELL" id 1)"

  [[ "$refs_out" == *"$d1"* ]]
  [[ "$refs_out" == *"$d2"* ]]
  [[ "$refs_out" != *" 3 1 "* ]]

  refs_out="$(SSH_ORIGINAL_COMMAND='list -ds' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$d1  3"* ]]
  [[ "$refs_out" == *"$d2  3"* ]]

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$d1  clean"* ]]
  [[ "$refs_out" == *"$d2  clean"* ]]

  refs_out="$(SSH_ORIGINAL_COMMAND='list' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$d1  main  3  1"* ]]
  [[ "$refs_out" == *"$d2  main  3  1"* ]]
}

@test "sht-shell list pads columns" {
  start_shtd

  d2="$(printf 'other' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  SSH_ORIGINAL_COMMAND='shelf create shelf1 20 20' "$BIN_SHELL" id 1 >/dev/null
  d1="$(printf 'same' | SSH_ORIGINAL_COMMAND='shelf1' "$BIN_SHELL" id 1 | tail -n1)"

  refs_out="$(SSH_ORIGINAL_COMMAND='list' "$BIN_SHELL" id 1)"

  [[ "$refs_out" == *"$d1  shelf1  4  1"* ]]
  [[ "$refs_out" == *"$d2  main    5  1"* ]]
}

@test "sht-shell quota shows total user usage" {
  start_shtd

  printf 'abc' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 >/dev/null

  quota_out="$(SSH_ORIGINAL_COMMAND='quota' "$BIN_SHELL" id 1)"
  [[ "$quota_out" == *"live      3/3221225472"* ]]
  [[ "$quota_out" == *"pending   3/4026531840"* ]]
  [[ "$quota_out" == *"reserved  0"* ]]
  [[ "$quota_out" == *"clean     1"* ]]
}

@test "sht-shell quota rejects extra arguments" {
  run env SSH_ORIGINAL_COMMAND='quota bogus' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht quota"* ]]
}

@test "sht-shell json mode supports common command output" {
  start_shtd

  upload_json="$(printf 'abc' | SSH_ORIGINAL_COMMAND='-- -j' "$BIN_SHELL" id 1)"
  digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$upload_json")"
  [ -n "$digest" ]

  cat_json="$(SSH_ORIGINAL_COMMAND="cat -j $digest" "$BIN_SHELL" id 1)"
  stat_json="$(SSH_ORIGINAL_COMMAND="-j stat $digest" "$BIN_SHELL" id 1)"
  list_json="$(SSH_ORIGINAL_COMMAND="list -j dt" "$BIN_SHELL" id 1)"
  quota_json="$(SSH_ORIGINAL_COMMAND="quota -j" "$BIN_SHELL" id 1)"

  python3 - "$upload_json" "$cat_json" "$stat_json" "$list_json" "$quota_json" "$digest" <<'PY'
import json
import sys

upload, cat, stat, refs, quota = [json.loads(arg) for arg in sys.argv[1:6]]
digest = sys.argv[6]

assert upload["digest"] == digest
assert upload["size"] == 3
assert cat["blobs"][0]["digest"] == digest
assert cat["blobs"][0]["content_base64"] == "YWJj"
assert stat["results"][0]["exists"] is True
assert refs["refs"][0]["digest"] == digest
assert quota["used_bytes"] == 3
PY
}

@test "sht-shell json digest commands read digests from stdin" {
  start_shtd

  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"

  stat_json="$(printf '%s\n%s\n' "$d1" "$d2" | SSH_ORIGINAL_COMMAND='stat -j' "$BIN_SHELL" id 1)"
  cat_json="$(printf '%s\n%s\n' "$d1" "$d2" | SSH_ORIGINAL_COMMAND='cat -j' "$BIN_SHELL" id 1)"

  python3 - "$stat_json" "$cat_json" "$d1" "$d2" <<'PY'
import json
import sys

stat, cat = [json.loads(arg) for arg in sys.argv[1:3]]
d1, d2 = sys.argv[3:5]
assert [row["digest"] for row in stat["results"]] == [d1, d2]
assert all(row["exists"] for row in stat["results"])
assert [row["digest"] for row in cat["blobs"]] == [d1, d2]
assert [row["content_base64"] for row in cat["blobs"]] == ["b25l", "dHdv"]
PY
}

@test "sht-shell explicit upload sentinel uploads stdin" {
  start_shtd

  digest="$(printf 'plain upload' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  cat_out="$(SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1)"
  [ "$cat_out" = "plain upload" ]

  SSH_ORIGINAL_COMMAND='shelf create app 20 20' "$BIN_SHELL" id 1 >/dev/null
  scoped="$(printf 'scoped upload' | SSH_ORIGINAL_COMMAND='app -- -j' "$BIN_SHELL" id 1)"
  scoped_digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$scoped")"
  [ -n "$scoped_digest" ]

  refs_out="$(SSH_ORIGINAL_COMMAND='app list d' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$scoped_digest"* ]]
}

@test "sht-shell json mode supports shelf command output" {
  start_shtd

  created="$(SSH_ORIGINAL_COMMAND='shelf create -j app1 20 20' "$BIN_SHELL" id 1)"
  listed="$(SSH_ORIGINAL_COMMAND='-j shelf list' "$BIN_SHELL" id 1)"
  defaulted="$(SSH_ORIGINAL_COMMAND='shelf -j default app1' "$BIN_SHELL" id 1)"
  SSH_ORIGINAL_COMMAND='shelf create app2 20 20' "$BIN_SHELL" id 1 >/dev/null

  python3 - "$created" "$listed" "$defaulted" <<'PY'
import json
import sys

created, listed, defaulted = [json.loads(arg) for arg in sys.argv[1:4]]
assert created["name"] == "app1"
assert any(shelf["name"] == "app1" for shelf in listed["shelves"])
assert defaulted["name"] == "app1"
assert defaulted["is_default"] is True
PY

  deleted="$(SSH_ORIGINAL_COMMAND='shelf delete app2 --force -j' "$BIN_SHELL" id 1)"
  python3 - "$deleted" <<'PY'
import json
import sys

deleted = json.loads(sys.argv[1])
assert deleted == {"deleted": True, "name": "app2"}
PY
}

@test "sht-shell json mode supports scoped release output" {
  start_shtd

  SSH_ORIGINAL_COMMAND='shelf create appj 20 20' "$BIN_SHELL" id 1 >/dev/null
  upload_json="$(printf 'scoped' | SSH_ORIGINAL_COMMAND='appj -- -j' "$BIN_SHELL" id 1)"
  digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$upload_json")"

  release_json="$(SSH_ORIGINAL_COMMAND="appj release -j $digest" "$BIN_SHELL" id 1)"

  python3 - "$release_json" "$digest" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
digest = sys.argv[2]
assert out["results"] == [{"digest": digest, "released": True, "status": 204}]
PY
}

@test "sht-shell json mode reports command errors as json" {
  start_shtd

  run env SSH_ORIGINAL_COMMAND='-j stat not-real' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["results"][0]["digest"] == "not-real"
assert out["results"][0]["exists"] is False
assert out["results"][0]["error"]["status"] == 404
PY

  run env SSH_ORIGINAL_COMMAND='list -j z' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert "unknown list field" in out["error"]["message"]
PY
}

@test "sht-shell json mode reports partial digest failures" {
  start_shtd

  digest="$(printf 'one' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"

  run bash -c "printf '%s\nmissing\n' '$digest' | SSH_ORIGINAL_COMMAND='stat -j' '$BIN_SHELL' id 1"
  [ "$status" -ne 0 ]
  python3 - "$output" "$digest" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
digest = sys.argv[2]
assert out["results"][0]["digest"] == digest
assert out["results"][0]["exists"] is True
assert out["results"][1]["digest"] == "missing"
assert out["results"][1]["exists"] is False
assert out["results"][1]["error"]["status"] == 404
PY

  run bash -c "printf '%s\nmissing\n' '$digest' | SSH_ORIGINAL_COMMAND='cat -j' '$BIN_SHELL' id 1"
  [ "$status" -ne 0 ]
  python3 - "$output" "$digest" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
digest = sys.argv[2]
assert out["blobs"][0]["digest"] == digest
assert out["blobs"][0]["content_base64"] == "b25l"
assert out["blobs"][1]["digest"] == "missing"
assert out["blobs"][1]["error"]["status"] == 404
PY
}

@test "sht-shell json mode reports daemon errors as json" {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_bytes = 1 WHERE id = 1"
  start_shtd

  run bash -c "printf abc | SSH_ORIGINAL_COMMAND='-- -j' '$BIN_SHELL' id 1"
  [ "$status" -ne 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["error"]["status"] == 413
assert "quota exceeded" in out["error"]["message"]
PY

  run env SSH_ORIGINAL_COMMAND='missing list -j' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  python3 - "$output" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["error"]["status"] == 400
assert "shelf not found" in out["error"]["message"]
PY
}

@test "sht-shell list shows released refs as dirty" {
  start_shtd

  digest="$(printf 'dirty' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  release_out="$(SSH_ORIGINAL_COMMAND="release $digest" "$BIN_SHELL" id 1)"
  [ "$release_out" = "released" ]

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest  dirty"* ]]

  run env SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell resumable upload commands roundtrip" {
  start_shtd

  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  manifest='{"digest":"'"$digest"'","size":11,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$digest"'","size":11}]}'

  status="$(printf '%s' "$manifest" | SSH_ORIGINAL_COMMAND='manifest' "$BIN_SHELL" id 1)"
  [[ "$status" == *'"missing":[0]'* ]]

  chunk_out="$(printf 'hello world' | SSH_ORIGINAL_COMMAND="upload $digest 0" "$BIN_SHELL" id 1)"
  [[ "$chunk_out" == *'"index":0'* ]]

  status="$(SSH_ORIGINAL_COMMAND="status $digest" "$BIN_SHELL" id 1)"
  [[ "$status" == *'"missing":[]'* ]]
  [[ "$status" == *'"complete":true'* ]]

  finalize_out="$(SSH_ORIGINAL_COMMAND="finalize $digest" "$BIN_SHELL" id 1)"
  [ "$finalize_out" = "$digest" ]

  cat_out="$(SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1)"
  [ "$cat_out" = "hello world" ]
}

@test "sht-shell finalize json returns full store response" {
  start_shtd

  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  manifest='{"digest":"'"$digest"'","size":11,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$digest"'","size":11}]}'

  printf '%s' "$manifest" | SSH_ORIGINAL_COMMAND='manifest -j' "$BIN_SHELL" id 1 >/dev/null
  printf 'hello world' | SSH_ORIGINAL_COMMAND="upload -j $digest 0" "$BIN_SHELL" id 1 >/dev/null
  finalized="$(SSH_ORIGINAL_COMMAND="finalize -j $digest" "$BIN_SHELL" id 1)"

  python3 - "$finalized" "$digest" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
digest = sys.argv[2]
assert out["digest"] == digest
assert out["size"] == 11
assert out["exists"] is False
PY
}

@test "sht-shell shelf commands enable scoped uploads and lists" {
  start_shtd

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" == *"main  0/3221225472  0/4026531840  0  enabled  default"* ]]

  created="$(SSH_ORIGINAL_COMMAND='shelf create app1 3 10' "$BIN_SHELL" id 1)"
  [ "$created" = "app1 3 10 enabled" ]

  run bash -c "printf abc | SSH_ORIGINAL_COMMAND=' ' \"$BIN_SHELL\" id 1"
  [ "$status" -ne 0 ]
  [[ "$output" == *"shelf required"* ]]
  [[ "$output" == *"sht shelf list"* ]]
  [[ "$output" == *"sht <shelf> <command>"* ]]

  digest="$(printf 'abc' | SSH_ORIGINAL_COMMAND='app1' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  duplicate="$(printf 'abc' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  [ "$duplicate" = "$digest" ]

  refs_out="$(SSH_ORIGINAL_COMMAND='app1 list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest  clean"* ]]

  refs_out="$(SSH_ORIGINAL_COMMAND='list' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest  app1  3  1"* ]]

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" == *"app1"* ]]
  [[ "$shelves" == *"3/3"* ]]
  [[ "$shelves" == *"3/10"* ]]
  [[ "$shelves" == *"1  enabled"* ]]

  SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1 >"$TEST_TMPDIR/cat.out"
  cmp <(printf 'abc\n') "$TEST_TMPDIR/cat.out"
}

@test "sht-shell scoped release only releases from the named shelf" {
  start_shtd

  SSH_ORIGINAL_COMMAND='shelf create a 20 20' "$BIN_SHELL" id 1 >/dev/null
  SSH_ORIGINAL_COMMAND='shelf create b 20 20' "$BIN_SHELL" id 1 >/dev/null

  digest="$(printf 'shared' | SSH_ORIGINAL_COMMAND='a' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]
  printf 'shared' | SSH_ORIGINAL_COMMAND='b' "$BIN_SHELL" id 1 >/dev/null

  release_out="$(SSH_ORIGINAL_COMMAND="a release $digest" "$BIN_SHELL" id 1)"
  [ "$release_out" = "released" ]

  refs_a="$(SSH_ORIGINAL_COMMAND='a list -dt' "$BIN_SHELL" id 1)"
  refs_b="$(SSH_ORIGINAL_COMMAND='b list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_a" == *"$digest  dirty"* ]]
  [[ "$refs_b" == *"$digest  clean"* ]]

  cat_out="$(SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1)"
  [ "$cat_out" = "shared" ]
}

@test "sht-shell shelf delete requires force and removes shelf" {
  start_shtd

  SSH_ORIGINAL_COMMAND='shelf create doomed 20 20' "$BIN_SHELL" id 1 >/dev/null
  digest="$(printf 'payload' | SSH_ORIGINAL_COMMAND='doomed' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  run env SSH_ORIGINAL_COMMAND='shelf delete doomed' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht shelf delete <name> --force"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf delete main --force' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"cannot delete default shelf"* ]]

  deleted="$(SSH_ORIGINAL_COMMAND='shelf delete doomed -f' "$BIN_SHELL" id 1)"
  [ "$deleted" = "deleted" ]

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" != *"doomed"* ]]

  run env SSH_ORIGINAL_COMMAND='doomed list -dt' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"shelf not found"* ]]
  [[ "$output" == *"sht shelf list"* ]]
  [[ "$output" == *"sht shelf create doomed <max_bytes> <max_pending_bytes>"* ]]

  run env SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell can rename shelves and set default" {
  start_shtd

  renamed="$(SSH_ORIGINAL_COMMAND='shelf rename main primary' "$BIN_SHELL" id 1)"
  [[ "$renamed" == *"primary 3221225472 4026531840 enabled default"* ]]

  digest="$(printf 'plain' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  SSH_ORIGINAL_COMMAND='shelf create app1 20 20' "$BIN_SHELL" id 1 >/dev/null
  scoped="$(printf 'scoped' | SSH_ORIGINAL_COMMAND='app1' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$scoped" ]

  changed="$(SSH_ORIGINAL_COMMAND='shelf default app1' "$BIN_SHELL" id 1)"
  [ "$changed" = "app1 default" ]

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" == *"app1"* ]]
  [[ "$shelves" == *"6/20"* ]]
  [[ "$shelves" == *"1  enabled  default"* ]]
  [[ "$shelves" == *"primary"* ]]
  [[ "$shelves" == *"5/3221225472"* ]]
  [[ "$shelves" == *"5/4026531840"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf delete app1 --force' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"cannot delete default shelf"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf rename primary app1' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"shelf already exists"* ]]
}

@test "sht-shell can recreate default name after default role moves" {
  start_shtd

  SSH_ORIGINAL_COMMAND='shelf create app1 20 20' "$BIN_SHELL" id 1 >/dev/null
  SSH_ORIGINAL_COMMAND='shelf default app1' "$BIN_SHELL" id 1 >/dev/null
  SSH_ORIGINAL_COMMAND='shelf delete main --force' "$BIN_SHELL" id 1 >/dev/null

  created="$(SSH_ORIGINAL_COMMAND='shelf create default 20 20' "$BIN_SHELL" id 1)"
  [ "$created" = "default 20 20 enabled" ]
}

@test "sht-shell alias commands create and read versioned aliases" {
  start_shtd

  run env SSH_ORIGINAL_COMMAND='help alias' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht alias set <namespace> <path> <digest>"* ]]

  created="$(SSH_ORIGINAL_COMMAND='alias ns create team' "$BIN_SHELL" id 1)"
  [ "$created" = "team 1" ]

  digest="$(printf 'payload' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  set_out="$(SSH_ORIGINAL_COMMAND="alias set team docs/readme $digest" "$BIN_SHELL" id 1)"
  version="$(awk '{print $3}' <<<"$set_out")"
  [ "$version" -gt 0 ]

  get_out="$(SSH_ORIGINAL_COMMAND='alias get team docs/readme' "$BIN_SHELL" id 1)"
  list_out="$(SSH_ORIGINAL_COMMAND='alias list team docs' "$BIN_SHELL" id 1)"
  history_out="$(SSH_ORIGINAL_COMMAND='alias history team docs/readme' "$BIN_SHELL" id 1)"
  cat_out="$(SSH_ORIGINAL_COMMAND='alias cat team docs/readme' "$BIN_SHELL" id 1)"

  [[ "$get_out" == *"docs/readme"* ]]
  [[ "$get_out" == *"$digest"* ]]
  [[ "$list_out" == *"team"* ]]
  [[ "$list_out" == *"docs/readme"* ]]
  [[ "$history_out" == *"$digest"* ]]
  [ "$cat_out" = "payload" ]
}

@test "sht-shell alias json commands return structured output" {
  start_shtd

  ns_json="$(SSH_ORIGINAL_COMMAND='alias ns create -j teamj' "$BIN_SHELL" id 1)"
  digest="$(printf 'json-payload' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  set_json="$(SSH_ORIGINAL_COMMAND="alias set -j teamj docs/file $digest -m \"first message\"" "$BIN_SHELL" id 1)"
  get_json="$(SSH_ORIGINAL_COMMAND='alias get -j teamj docs/file' "$BIN_SHELL" id 1)"
  list_json="$(SSH_ORIGINAL_COMMAND='alias list -j teamj docs' "$BIN_SHELL" id 1)"
  history_json="$(SSH_ORIGINAL_COMMAND='alias history -j teamj docs/file' "$BIN_SHELL" id 1)"
  cat_json="$(SSH_ORIGINAL_COMMAND='alias cat -j teamj docs/file' "$BIN_SHELL" id 1)"

  python3 - "$ns_json" "$set_json" "$get_json" "$list_json" "$history_json" "$cat_json" "$digest" <<'PY'
import json
import sys

ns, set_out, get_out, list_out, history, cat = [json.loads(arg) for arg in sys.argv[1:7]]
digest = sys.argv[7]

assert ns["name"] == "teamj"
assert set_out["digest"] == digest
assert set_out["message"] == "first message"
assert get_out["path"] == "docs/file"
assert get_out["digest"] == digest
assert list_out["aliases"][0]["digest"] == digest
assert history["versions"][0]["digest"] == digest
assert history["versions"][0]["message"] == "first message"
assert cat["blobs"][0]["digest"] == digest
assert cat["blobs"][0]["content_base64"] == "anNvbi1wYXlsb2Fk"
PY
}

@test "sht-shell alias set prints friendly text conflicts" {
  start_shtd

  SSH_ORIGINAL_COMMAND='alias ns create friendly' "$BIN_SHELL" id 1 >/dev/null
  d1="$(printf 'one' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  d2="$(printf 'two' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"

  set1="$(SSH_ORIGINAL_COMMAND="alias set friendly docs/file $d1 -m \"initial version\"" "$BIN_SHELL" id 1)"
  version1="$(awk '{print $3}' <<<"$set1")"

  same_out="$(SSH_ORIGINAL_COMMAND="alias set friendly docs/file $d1" "$BIN_SHELL" id 1)"
  [ "$same_out" = "up to date" ]

  set2="$(SSH_ORIGINAL_COMMAND="alias set friendly docs/file $d2 --expect $version1 -m \"second version\"" "$BIN_SHELL" id 1)"
  version2="$(awk '{print $3}' <<<"$set2")"
  [ "$version2" -gt "$version1" ]

  run env SSH_ORIGINAL_COMMAND="alias set friendly docs/file $d1 --expect $version1" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == "stale alias version: current is $version2" ]]

  history_json="$(SSH_ORIGINAL_COMMAND='alias history -j friendly docs/file' "$BIN_SHELL" id 1)"
  python3 - "$history_json" <<'PY'
import json
import sys

history = json.loads(sys.argv[1])
assert [v["message"] for v in history["versions"]] == ["second version", "initial version"]
PY
}

@test "sht-shell alias set json keeps structured daemon conflict" {
  start_shtd

  SSH_ORIGINAL_COMMAND='alias ns create jsonconflict' "$BIN_SHELL" id 1 >/dev/null
  digest="$(printf 'same' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  set1="$(SSH_ORIGINAL_COMMAND="alias set jsonconflict docs/file $digest" "$BIN_SHELL" id 1)"
  version1="$(awk '{print $3}' <<<"$set1")"

  run env SSH_ORIGINAL_COMMAND="alias set -j jsonconflict docs/file $digest" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  python3 - "$output" "$version1" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out["error"] == "alias conflict"
assert out["current_version"] == int(sys.argv[2])
PY
}

@test "sht-shell alias grants let another user update recursively" {
  start_shtd

  SSH_ORIGINAL_COMMAND='alias ns create shared' "$BIN_SHELL" id 1 >/dev/null
  d1="$(printf 'alpha' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 1 | tail -n1)"
  set1="$(SSH_ORIGINAL_COMMAND="alias set shared docs/file $d1" "$BIN_SHELL" id 1)"
  version1="$(awk '{print $3}' <<<"$set1")"

  run env SSH_ORIGINAL_COMMAND='alias cat shared docs/file' "$BIN_SHELL" id 2
  [ "$status" -ne 0 ]
  [[ "$output" == *"alias permission denied"* ]]

  grant="$(SSH_ORIGINAL_COMMAND='alias grant shared docs test-user-2 write' "$BIN_SHELL" id 1)"
  [ "$grant" = "docs test-user-2 write" ]

  d2="$(printf 'beta' | SSH_ORIGINAL_COMMAND='--' "$BIN_SHELL" id 2 | tail -n1)"
  set2="$(SSH_ORIGINAL_COMMAND="alias set shared docs/file $d2 --expect $version1" "$BIN_SHELL" id 2)"
  [[ "$set2" == *"$d2"* ]]

  cat_out="$(SSH_ORIGINAL_COMMAND='alias cat shared docs/file' "$BIN_SHELL" id 2)"
  [ "$cat_out" = "beta" ]

  revoke_json="$(SSH_ORIGINAL_COMMAND='alias revoke -j shared docs test-user-2' "$BIN_SHELL" id 1)"
  python3 - "$revoke_json" <<'PY'
import json
import sys

out = json.loads(sys.argv[1])
assert out == {"revoked": True, "namespace": "shared", "path": "docs", "user": "test-user-2"}
PY
}

@test "sht-shell top-level shelf admin verbs point to shelf usage" {
  run env SSH_ORIGINAL_COMMAND='set-default app1' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht shelf default"* ]]

  run env SSH_ORIGINAL_COMMAND='set-default' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht shelf default"* ]]
}

@test "sht-shell rejects unknown command" {
  run env SSH_ORIGINAL_COMMAND='wat nope' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"unknown scoped command"* ]]
}
