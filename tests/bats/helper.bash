#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_SHTD="$ROOT_DIR/bin/shtd"
BIN_SHELL="$ROOT_DIR/bin/sht-shell"

setup_test_env() {
  TEST_TMPDIR="$(mktemp -d)"
  export TEST_TMPDIR
  export SHT_SOCK_DIR="$TEST_TMPDIR/sht.sock"
  export SHT_BLOB_DIR="$TEST_TMPDIR/blobs"
  export SHT_TMP_DIR="$TEST_TMPDIR/tmp"
}

teardown_test_env() {
  if [[ -n "${SHTD_PID:-}" ]]; then
    kill "$SHTD_PID" >/dev/null 2>&1 || true
    wait "$SHTD_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "${TEST_TMPDIR:-}"
}

start_shtd() {
  "$BIN_SHTD" >"$TEST_TMPDIR/shtd.log" 2>&1 &
  SHTD_PID=$!
  export SHTD_PID

  for _ in $(seq 1 50); do
    [[ -S "$SHT_SOCK_DIR" ]] && return 0
    sleep 0.05
  done

  echo "shtd failed to start" >&2
  [[ -f "$TEST_TMPDIR/shtd.log" ]] && cat "$TEST_TMPDIR/shtd.log" >&2
  return 1
}
