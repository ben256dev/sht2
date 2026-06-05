#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
  export SHT_AUTHORIZED_KEYS_PATH="$TEST_TMPDIR/authorized_keys"
  GATEWAY_PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
  export GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT"
  SHT_GATEWAY_ADDR="127.0.0.1:$GATEWAY_PORT" \
    SHT_GATEWAY_DEV_AUTH=1 \
    "$BIN_GATEWAY" >"$TEST_TMPDIR/sht-gateway.log" 2>&1 &
  GATEWAY_PID=$!
  export GATEWAY_PID

  for _ in $(seq 1 50); do
    if curl -sS "$GATEWAY_URL/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done

  echo "sht-gateway failed to start" >&2
  cat "$TEST_TMPDIR/sht-gateway.log" >&2
  return 1
}

teardown() {
  if [[ -n "${GATEWAY_PID:-}" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
  fi
  teardown_test_env
}

dev_curl() {
  curl -sS \
    -H 'X-SHT-Dev-Subject: kc-alice-subject' \
    -H 'X-SHT-Dev-Username: alice' \
    "$@"
}

@test "sht-gateway health check works without auth" {
  body="$(curl -sS "$GATEWAY_URL/healthz")"
  [ "$body" = '{"ok":true}' ]
}

@test "sht-gateway me creates and reuses mapped sht user" {
  body="$(dev_curl "$GATEWAY_URL/me")"
  [[ "$body" == *'"name":"alice"'* ]]
  [[ "$body" == *'"external_subject":"kc-alice-subject"'* ]]

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE external_subject = 'kc-alice-subject';")"
  [ -n "$user_id" ]
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM shelves WHERE user_id = $user_id AND name = 'main' AND is_default = 1;")" = "1" ]
  ns_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM alias_namespaces WHERE name = 'alice' AND owner_user_id = $user_id;")"
  [ -n "$ns_id" ]
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT role FROM alias_grants WHERE namespace_id = $ns_id AND user_id = $user_id AND path = '';")" = "admin" ]

  body_again="$(dev_curl "$GATEWAY_URL/me")"
  id_again="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["user"]["id"])' "$body_again")"
  [ "$id_again" = "$user_id" ]
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM users WHERE external_subject = 'kc-alice-subject';")" = "1" ]
}

@test "sht-gateway keys start empty for mapped user" {
  body="$(dev_curl "$GATEWAY_URL/keys")"
  [ "$body" = '{"keys":[]}' ]
}

@test "sht-gateway key registration stores key and syncs authorized_keys" {
  body="$(dev_curl \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"laptop","public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGatewayAlice alice@example"}')"

  [[ "$body" == *'"name":"laptop"'* ]]
  [[ "$body" == *'"public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGatewayAlice alice@example"'* ]]
  key_id="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["key"]["id"])' "$body")"
  [ -n "$key_id" ]

  keys="$(dev_curl "$GATEWAY_URL/keys")"
  [[ "$keys" == *'"id":'"$key_id"* ]]
  [[ "$keys" == *'"user":"alice"'* ]]
  grep -q '^command="sht-shell id '"$key_id"'",restrict ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGatewayAlice alice@example$' "$SHT_AUTHORIZED_KEYS_PATH"
}

@test "sht-gateway rejects duplicate and invalid keys as json errors" {
  dev_curl \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"laptop","public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDupe dupe@example"}' >/dev/null

  code="$(dev_curl \
    -o "$TEST_TMPDIR/dupe.out" \
    -w "%{http_code}" \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"desktop","public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDupe dupe@example"}')"
  [ "$code" = "409" ]
  grep -q '"public key already exists"' "$TEST_TMPDIR/dupe.out"

  code="$(dev_curl \
    -o "$TEST_TMPDIR/bad.out" \
    -w "%{http_code}" \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"bad","public_key":"not-a-key"}')"
  [ "$code" = "400" ]
  grep -q '"unsupported public key type"' "$TEST_TMPDIR/bad.out"
}

@test "sht-gateway key list is scoped to authenticated user" {
  dev_curl \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"alice-key","public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAliceScoped alice@example"}' >/dev/null

  curl -sS \
    -H 'X-SHT-Dev-Subject: kc-bob-subject' \
    -H 'X-SHT-Dev-Username: bob' \
    -H 'Content-Type: application/json' \
    -X POST "$GATEWAY_URL/keys" \
    --data-binary '{"name":"bob-key","public_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBobScoped bob@example"}' >/dev/null

  alice_keys="$(dev_curl "$GATEWAY_URL/keys")"
  [[ "$alice_keys" == *"AliceScoped"* ]]
  [[ "$alice_keys" != *"BobScoped"* ]]

  bob_keys="$(curl -sS -H 'X-SHT-Dev-Subject: kc-bob-subject' -H 'X-SHT-Dev-Username: bob' "$GATEWAY_URL/keys")"
  [[ "$bob_keys" == *"BobScoped"* ]]
  [[ "$bob_keys" != *"AliceScoped"* ]]
}

@test "sht-gateway rejects authenticated endpoints without auth" {
  code="$(curl -sS -o "$TEST_TMPDIR/noauth.out" -w "%{http_code}" "$GATEWAY_URL/me")"
  [ "$code" = "401" ]
  grep -q '"missing dev subject"' "$TEST_TMPDIR/noauth.out"
}
