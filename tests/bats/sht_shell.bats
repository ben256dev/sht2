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
  [[ "$output" == *"sht cat     [<digest> ...]"* ]]
  [[ "$output" == *"sht list [-dhskcta]"* ]]
  [[ "$output" == *"sht quota"* ]]
}

@test "sht-shell list help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='list -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [-dhskcta]"* ]]
  [[ "$output" == *"-d  digest"* ]]
  [[ "$output" == *"-h  shelf"* ]]
  [[ "$output" == *"-t  state"* ]]
  [[ "$output" == *"sht list -dht"* ]]
}

@test "sht-shell scoped list help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='app1 list -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [-dhskcta]"* ]]
  [[ "$output" == *"sht <shelf> list -dt"* ]]
}

@test "sht-shell digest command help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='release -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht release [<digest> ...]"* ]]
  [[ "$output" == *"digests are read from stdin"* ]]
}

@test "sht-shell shelf help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='shelf -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht shelf list"* ]]
  [[ "$output" == *"sht shelf set-default <name>"* ]]
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
  [[ "$refs_out" == *"$digest  default  18  1"* ]]
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

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dht' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$d1  default  clean"* ]]
  [[ "$refs_out" == *"$d2  default  clean"* ]]
}

@test "sht-shell list pads columns" {
  start_shtd

  d2="$(printf 'other' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  SSH_ORIGINAL_COMMAND='shelf create shelf1 20 20' "$BIN_SHELL" id 1 >/dev/null
  d1="$(printf 'same' | SSH_ORIGINAL_COMMAND='shelf1' "$BIN_SHELL" id 1 | tail -n1)"

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dht' "$BIN_SHELL" id 1)"

  [[ "$refs_out" == *"$d1  shelf1   clean"* ]]
  [[ "$refs_out" == *"$d2  default  clean"* ]]
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

@test "sht-shell shelf commands enable scoped uploads and lists" {
  start_shtd

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" == *"default  0/3221225472  0/4026531840  0  enabled  default"* ]]

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

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dht' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest  app1  clean"* ]]

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

  run env SSH_ORIGINAL_COMMAND='shelf delete default --force' "$BIN_SHELL" id 1
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

  renamed="$(SSH_ORIGINAL_COMMAND='shelf rename default main' "$BIN_SHELL" id 1)"
  [[ "$renamed" == *"main 3221225472 4026531840 enabled default"* ]]

  digest="$(printf 'plain' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  SSH_ORIGINAL_COMMAND='shelf create app1 20 20' "$BIN_SHELL" id 1 >/dev/null
  scoped="$(printf 'scoped' | SSH_ORIGINAL_COMMAND='app1' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$scoped" ]

  changed="$(SSH_ORIGINAL_COMMAND='shelf set-default app1' "$BIN_SHELL" id 1)"
  [ "$changed" = "app1 default" ]

  shelves="$(SSH_ORIGINAL_COMMAND='shelf list' "$BIN_SHELL" id 1)"
  [[ "$shelves" == *"app1"* ]]
  [[ "$shelves" == *"6/20"* ]]
  [[ "$shelves" == *"1  enabled  default"* ]]
  [[ "$shelves" == *"main"* ]]
  [[ "$shelves" == *"5/3221225472"* ]]
  [[ "$shelves" == *"5/4026531840"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf delete app1 --force' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"cannot delete default shelf"* ]]

  run env SSH_ORIGINAL_COMMAND='shelf rename main app1' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"shelf already exists"* ]]
}

@test "sht-shell can recreate default name after default role moves" {
  start_shtd

  SSH_ORIGINAL_COMMAND='shelf create app1 20 20' "$BIN_SHELL" id 1 >/dev/null
  SSH_ORIGINAL_COMMAND='shelf set-default app1' "$BIN_SHELL" id 1 >/dev/null
  SSH_ORIGINAL_COMMAND='shelf delete default --force' "$BIN_SHELL" id 1 >/dev/null

  created="$(SSH_ORIGINAL_COMMAND='shelf create default 20 20' "$BIN_SHELL" id 1)"
  [ "$created" = "default 20 20 enabled" ]
}

@test "sht-shell top-level shelf admin verbs point to shelf usage" {
  run env SSH_ORIGINAL_COMMAND='set-default app1' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht shelf set-default"* ]]

  run env SSH_ORIGINAL_COMMAND='set-default' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"usage: sht shelf set-default"* ]]
}

@test "sht-shell rejects unknown command" {
  run env SSH_ORIGINAL_COMMAND='wat nope' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"unknown scoped command"* ]]
}
