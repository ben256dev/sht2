#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
  export SHT_AUTHORIZED_KEYS_PATH="$TEST_TMPDIR/authorized_keys"
  start_shtd
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
  [[ "$body" == *'"kind":"external"'* ]]
  [[ "$body" == *'"identity_provider":"keycloak"'* ]]
  [[ "$body" == *'"external_subject":"kc-alice-subject"'* ]]

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE external_subject = 'kc-alice-subject';")"
  [ -n "$user_id" ]
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT kind, identity_provider FROM users WHERE id = $user_id;")" = "external|keycloak" ]
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

@test "sht-gateway uploads blobs for authenticated users and serves bytes publicly" {
  resp="$(printf 'gateway bytes' | dev_curl -X POST "$GATEWAY_URL/blob" --data-binary @-)"
  digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$resp")"

  [ -n "$digest" ]

  body="$(curl -sS "$GATEWAY_URL/blob/$digest")"
  [ "$body" = "gateway bytes" ]

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE external_subject = 'kc-alice-subject';")"
  gateway_keys="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM key_ids WHERE user_id = $user_id AND name = 'gateway' AND public_key IS NULL;")"
  ref_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_refs WHERE user_id = $user_id AND digest = '$digest';")"

  [ "$gateway_keys" = "1" ]
  [ "$ref_count" = "1" ]
}

@test "sht-gateway public blob HEAD returns length without auth" {
  resp="$(printf 'head body' | dev_curl -X POST "$GATEWAY_URL/blob" --data-binary @-)"
  digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$resp")"

  code="$(curl -sS -D "$TEST_TMPDIR/head.headers" -o /dev/null -w "%{http_code}" -I "$GATEWAY_URL/blob/$digest")"
  [ "$code" = "200" ]
  tr -d '\r' < "$TEST_TMPDIR/head.headers" | grep -qi '^Content-Length: 9$'
}

@test "sht-gateway public blob rejects invalid and missing digests" {
  code="$(curl -sS -o "$TEST_TMPDIR/invalid.out" -w "%{http_code}" "$GATEWAY_URL/blob/not-a-digest")"
  [ "$code" = "400" ]
  grep -q 'invalid digest' "$TEST_TMPDIR/invalid.out"

  code="$(curl -sS -o "$TEST_TMPDIR/missing.out" -w "%{http_code}" "$GATEWAY_URL/blob/10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ")"
  [ "$code" = "404" ]
  grep -q 'blob not found' "$TEST_TMPDIR/missing.out"
}

@test "sht-gateway rejects unauthenticated blob upload" {
  code="$(printf 'nope' | curl -sS -o "$TEST_TMPDIR/noauth-upload.out" -w "%{http_code}" -X POST "$GATEWAY_URL/blob" --data-binary @-)"
  [ "$code" = "401" ]
  grep -q '"missing dev subject"' "$TEST_TMPDIR/noauth-upload.out"
}

@test "sht-gateway reuses one internal gateway key across uploads" {
  printf 'first' | dev_curl -X POST "$GATEWAY_URL/blob" --data-binary @- >/dev/null
  printf 'second' | dev_curl -X POST "$GATEWAY_URL/blob" --data-binary @- >/dev/null

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE external_subject = 'kc-alice-subject';")"
  gateway_keys="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM key_ids WHERE user_id = $user_id AND name = 'gateway' AND public_key IS NULL;")"
  ref_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_refs WHERE user_id = $user_id;")"

  [ "$gateway_keys" = "1" ]
  [ "$ref_count" = "2" ]

  keys="$(dev_curl "$GATEWAY_URL/keys")"
  [ "$keys" = '{"keys":[]}' ]
}

@test "sht-gateway upload honors shelf query and header" {
  dev_curl "$GATEWAY_URL/me" >/dev/null
  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE external_subject = 'kc-alice-subject';")"
  sqlite3 "$SHT_DB_PATH" "
    UPDATE users SET multi_shelf_enabled = 1 WHERE id = $user_id;
    INSERT INTO shelves (user_id, name, enabled, is_default, max_bytes, max_pending_bytes, pending_bytes)
    VALUES ($user_id, 'browser', 1, 0, 1000, 1250, 0);
    INSERT INTO shelves (user_id, name, enabled, is_default, max_bytes, max_pending_bytes, pending_bytes)
    VALUES ($user_id, 'header', 1, 0, 1000, 1250, 0);
  "

  resp="$(printf 'query shelf' | dev_curl -X POST "$GATEWAY_URL/blob?shelf=browser" --data-binary @-)"
  query_digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$resp")"

  resp="$(printf 'header shelf' | dev_curl -H 'X-SHT-Shelf: header' -X POST "$GATEWAY_URL/blob?shelf=browser" --data-binary @-)"
  header_digest="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["digest"])' "$resp")"

  query_shelf="$(sqlite3 "$SHT_DB_PATH" "SELECT shelves.name FROM blob_refs JOIN shelves ON shelves.id = blob_refs.shelf_id WHERE blob_refs.digest = '$query_digest';")"
  header_shelf="$(sqlite3 "$SHT_DB_PATH" "SELECT shelves.name FROM blob_refs JOIN shelves ON shelves.id = blob_refs.shelf_id WHERE blob_refs.digest = '$header_digest';")"

  [ "$query_shelf" = "browser" ]
  [ "$header_shelf" = "header" ]
}

@test "sht-gateway proxies resumable upload flow" {
  digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  chunk_digest="10mB76cKDIgLjYwZhdB128v2ebmaX5kU5ar5a4ManiQ"
  manifest='{"digest":"'"$digest"'","size":11,"chunk_size":8388608,"chunks":[{"index":0,"digest":"'"$chunk_digest"'","size":11}]}'

  status="$(printf '%s' "$manifest" | dev_curl -H 'Content-Type: application/json' -X POST "$GATEWAY_URL/uploads" --data-binary @-)"
  [[ "$status" == *'"missing":[0]'* ]]
  [[ "$status" == *'"complete":false'* ]]

  chunk_resp="$(printf 'hello world' | dev_curl -X PUT "$GATEWAY_URL/uploads/$digest/chunks/0" --data-binary @-)"
  [[ "$chunk_resp" == *'"index":0'* ]]

  status="$(dev_curl "$GATEWAY_URL/uploads/$digest")"
  [[ "$status" == *'"uploaded":[0]'* ]]
  [[ "$status" == *'"complete":true'* ]]

  finalized="$(dev_curl -X POST "$GATEWAY_URL/uploads/$digest/finalize")"
  body="$(curl -sS "$GATEWAY_URL/blob/$digest")"

  [[ "$finalized" == *'"digest":"'"$digest"'"'* ]]
  [ "$body" = "hello world" ]
}
