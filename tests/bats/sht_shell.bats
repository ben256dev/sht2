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

@test "sht-shell rejects unknown command" {
  run env SSH_ORIGINAL_COMMAND='wat' "$BIN_SHELL" id 1
  [ "$status" -ne 0 ]
  [[ "$output" == *"unknown command"* ]]
}
