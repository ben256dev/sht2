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
  [[ "$output" == *"sht list [-dskcta]"* ]]
}

@test "sht-shell list help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='list -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht list [-dskcta]"* ]]
  [[ "$output" == *"-d  digest"* ]]
  [[ "$output" == *"-t  state"* ]]
  [[ "$output" == *"sht list -dt"* ]]
}

@test "sht-shell digest command help prints command usage" {
  run env SSH_ORIGINAL_COMMAND='release -h' "$BIN_SHELL" id 1
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht release [<digest> ...]"* ]]
  [[ "$output" == *"digests are read from stdin"* ]]
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
  [[ "$refs_out" == *"$digest 18 1 "* ]]
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
  [[ "$refs_out" == *"$d1 3"* ]]
  [[ "$refs_out" == *"$d2 3"* ]]

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$d1 clean"* ]]
  [[ "$refs_out" == *"$d2 clean"* ]]
}

@test "sht-shell list shows released refs as dirty" {
  start_shtd

  digest="$(printf 'dirty' | SSH_ORIGINAL_COMMAND=' ' "$BIN_SHELL" id 1 | tail -n1)"
  [ -n "$digest" ]

  release_out="$(SSH_ORIGINAL_COMMAND="release $digest" "$BIN_SHELL" id 1)"
  [ "$release_out" = "released" ]

  refs_out="$(SSH_ORIGINAL_COMMAND='list -dt' "$BIN_SHELL" id 1)"
  [[ "$refs_out" == *"$digest dirty"* ]]

  run env SSH_ORIGINAL_COMMAND="cat $digest" "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"not found"* ]]
}

@test "sht-shell rejects unknown command" {
  run env SSH_ORIGINAL_COMMAND='wat' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"unknown command"* ]]
}
