#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

socket_mode() {
  stat -c '%a' "$SHT_SOCK_DIR"
}

@test "shtd creates socket with mode 660 by default" {
  start_shtd

  [ "$(socket_mode)" = "660" ]
}

@test "shtd honors SHT_SOCK_MODE override" {
  export SHT_SOCK_MODE=0600
  start_shtd

  [ "$(socket_mode)" = "600" ]
}

@test "shtd rejects invalid SHT_SOCK_MODE" {
  export SHT_SOCK_MODE=not-octal

  "$BIN_SHTD" >"$TEST_TMPDIR/shtd.log" 2>&1 &
  pid=$!

  if wait "$pid"; then
    status=0
  else
    status=$?
  fi

  [ "$status" -ne 0 ]
  [[ ! -S "$SHT_SOCK_DIR" ]]
  grep -q 'invalid SHT_SOCK_MODE' "$TEST_TMPDIR/shtd.log"
}
