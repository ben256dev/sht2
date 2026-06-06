#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
  export SHT_AUTHORIZED_KEYS_PATH="$TEST_TMPDIR/authorized_keys"
}

teardown() {
  teardown_test_env
}

seed_db_backed_keys() {
  sqlite3 "$SHT_DB_PATH" <<'SQL'
ALTER TABLE key_ids ADD COLUMN public_key TEXT;
UPDATE key_ids SET public_key = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEnabledOne enabled-one@example' WHERE id = 1;
UPDATE key_ids SET public_key = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDisabled disabled@example', enabled = 0 WHERE id = 2;
UPDATE key_ids SET public_key = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEnabledTwo enabled-two@example' WHERE id = 3;
SQL
}

write_public_key() {
  local path="$1"
  local key="$2"
  printf '%s\n' "$key" > "$path"
}

@test "sht-admin sync renders forced-command authorized_keys from enabled DB keys" {
  seed_db_backed_keys

  run bash -c '[[ -x "$1" ]] || exit 42; "$1" authorized-keys sync --output "$2"' _ "$BIN_ADMIN" "$SHT_AUTHORIZED_KEYS_PATH"
  [ "$status" -eq 0 ]

  [ -f "$SHT_AUTHORIZED_KEYS_PATH" ]
  line_count="$(wc -l < "$SHT_AUTHORIZED_KEYS_PATH" | tr -d ' ')"
  [ "$line_count" = "2" ]

  grep -q '^command="sht-shell id 1",restrict ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEnabledOne enabled-one@example$' "$SHT_AUTHORIZED_KEYS_PATH"
  grep -q '^command="sht-shell id 3",restrict ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEnabledTwo enabled-two@example$' "$SHT_AUTHORIZED_KEYS_PATH"
  ! grep -q 'disabled@example' "$SHT_AUTHORIZED_KEYS_PATH"
}

@test "sht-admin sync writes authorized_keys atomically over stale contents" {
  seed_db_backed_keys
  printf '%s\n' 'stale-key-line' > "$SHT_AUTHORIZED_KEYS_PATH"

  run bash -c '[[ -x "$1" ]] || exit 42; "$1" authorized-keys sync --output "$2"' _ "$BIN_ADMIN" "$SHT_AUTHORIZED_KEYS_PATH"
  [ "$status" -eq 0 ]

  ! grep -q 'stale-key-line' "$SHT_AUTHORIZED_KEYS_PATH"
  grep -q '^command="sht-shell id 1",restrict ' "$SHT_AUTHORIZED_KEYS_PATH"
  grep -q '^command="sht-shell id 3",restrict ' "$SHT_AUTHORIZED_KEYS_PATH"
}

@test "sht-admin user create creates complete usable user state" {
  run "$BIN_ADMIN" user create alice
  [ "$status" -eq 0 ]
  [[ "$output" == *"alice enabled"* ]]

  user_row="$(sqlite3 "$SHT_DB_PATH" "SELECT id, enabled, max_bytes, max_pending_bytes, max_simple_upload_bytes, multi_shelf_enabled, kind, identity_provider FROM users WHERE name = 'alice';")"
  [[ "$user_row" == *"|1|3221225472|4026531840|67108864|0|internal|local" ]]

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE name = 'alice';")"
  shelf_row="$(sqlite3 "$SHT_DB_PATH" "SELECT name, enabled, is_default, max_bytes, max_pending_bytes FROM shelves WHERE user_id = $user_id;")"
  [ "$shelf_row" = "main|1|1|3221225472|4026531840" ]

  ns_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM alias_namespaces WHERE name = 'alice' AND owner_user_id = $user_id;")"
  [ -n "$ns_id" ]
  grant_row="$(sqlite3 "$SHT_DB_PATH" "SELECT path, user_id, role FROM alias_grants WHERE namespace_id = $ns_id;")"
  [ "$grant_row" = "|$user_id|admin" ]
}

@test "sht-admin service create creates service user state" {
  run "$BIN_ADMIN" service create shthub
  [ "$status" -eq 0 ]
  [[ "$output" == *"shthub enabled service system"* ]]

  user_row="$(sqlite3 "$SHT_DB_PATH" "SELECT id, enabled, kind, identity_provider, COALESCE(external_subject, '') FROM users WHERE name = 'shthub';")"
  [[ "$user_row" == *"|1|service|system|" ]]

  user_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM users WHERE name = 'shthub';")"
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM shelves WHERE user_id = $user_id AND name = 'main' AND is_default = 1;")" = "1" ]
  ns_id="$(sqlite3 "$SHT_DB_PATH" "SELECT id FROM alias_namespaces WHERE name = 'shthub' AND owner_user_id = $user_id;")"
  [ -n "$ns_id" ]
  [ "$(sqlite3 "$SHT_DB_PATH" "SELECT role FROM alias_grants WHERE namespace_id = $ns_id AND user_id = $user_id AND path = '';")" = "admin" ]
}

@test "sht-admin user list shows kind and provider" {
  "$BIN_ADMIN" user create alice >/dev/null
  "$BIN_ADMIN" service create shthub >/dev/null

  users="$("$BIN_ADMIN" user list)"
  [[ "$users" == *"alice enabled internal local"* ]]
  [[ "$users" == *"shthub enabled service system"* ]]
}

@test "sht-admin user create rejects duplicate and invalid names" {
  "$BIN_ADMIN" user create alice >/dev/null

  run "$BIN_ADMIN" user create alice
  [ "$status" -ne 0 ]
  [[ "$output" == *"user already exists"* ]]

  run "$BIN_ADMIN" user create 'bad/name'
  [ "$status" -ne 0 ]
  [[ "$output" == *"invalid user name"* ]]
}

@test "sht-admin key add stores public key and key list can filter by user" {
  "$BIN_ADMIN" user create alice >/dev/null
  write_public_key "$TEST_TMPDIR/alice.pub" 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAliceKey alice@example'

  run "$BIN_ADMIN" key add alice laptop "$TEST_TMPDIR/alice.pub"
  [ "$status" -eq 0 ]
  [[ "$output" == *"alice laptop enabled"* ]]
  key_id="$(awk '{print $1}' <<<"$output")"

  stored="$(sqlite3 "$SHT_DB_PATH" "SELECT users.name, key_ids.name, key_ids.public_key, key_ids.enabled FROM key_ids JOIN users ON users.id = key_ids.user_id WHERE key_ids.id = $key_id;")"
  [ "$stored" = "alice|laptop|ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAliceKey alice@example|1" ]

  all_keys="$("$BIN_ADMIN" key list)"
  [[ "$all_keys" == *"$key_id alice laptop enabled ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAliceKey alice@example"* ]]

  alice_keys="$("$BIN_ADMIN" key list alice)"
  [[ "$alice_keys" == *"$key_id alice laptop enabled"* ]]
  [[ "$alice_keys" != *"test-user-1"* ]]
}

@test "sht-admin key add rejects duplicate and invalid public keys" {
  "$BIN_ADMIN" user create alice >/dev/null
  write_public_key "$TEST_TMPDIR/alice.pub" 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDupeKey alice@example'
  "$BIN_ADMIN" key add alice laptop "$TEST_TMPDIR/alice.pub" >/dev/null

  run "$BIN_ADMIN" key add alice desktop "$TEST_TMPDIR/alice.pub"
  [ "$status" -ne 0 ]
  [[ "$output" == *"public key already exists"* ]]

  write_public_key "$TEST_TMPDIR/bad.pub" 'not-a-public-key'
  run "$BIN_ADMIN" key add alice bad "$TEST_TMPDIR/bad.pub"
  [ "$status" -ne 0 ]
  [[ "$output" == *"unsupported public key type"* ]]

  run "$BIN_ADMIN" key add alice 'bad/name' "$TEST_TMPDIR/alice.pub"
  [ "$status" -ne 0 ]
  [[ "$output" == *"invalid key name"* ]]
}

@test "sht-admin key and user disable remove keys from generated authorized_keys" {
  "$BIN_ADMIN" user create alice >/dev/null
  "$BIN_ADMIN" user create bob >/dev/null
  write_public_key "$TEST_TMPDIR/alice.pub" 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAliceKey alice@example'
  write_public_key "$TEST_TMPDIR/bob.pub" 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBobKey bob@example'
  alice_key="$("$BIN_ADMIN" key add alice laptop "$TEST_TMPDIR/alice.pub" | awk '{print $1}')"
  "$BIN_ADMIN" key add bob laptop "$TEST_TMPDIR/bob.pub" >/dev/null

  "$BIN_ADMIN" key disable "$alice_key" >/dev/null
  "$BIN_ADMIN" authorized-keys sync --output "$SHT_AUTHORIZED_KEYS_PATH"
  ! grep -q 'alice@example' "$SHT_AUTHORIZED_KEYS_PATH"
  grep -q 'bob@example' "$SHT_AUTHORIZED_KEYS_PATH"

  "$BIN_ADMIN" user disable bob >/dev/null
  "$BIN_ADMIN" authorized-keys sync --output "$SHT_AUTHORIZED_KEYS_PATH"
  ! grep -q 'bob@example' "$SHT_AUTHORIZED_KEYS_PATH"
}

@test "sht-admin usage messages cover admin command groups" {
  run "$BIN_ADMIN" help
  [ "$status" -eq 0 ]
  [[ "$output" == *"sht-admin user create <name>"* ]]
  [[ "$output" == *"sht-admin service create <name>"* ]]
  [[ "$output" == *"sht-admin key add <user> <name> <public-key-file>"* ]]

  run "$BIN_ADMIN" user
  [ "$status" -ne 0 ]
  [[ "$output" == *"sht-admin user create <name>"* ]]

  run "$BIN_ADMIN" key
  [ "$status" -ne 0 ]
  [[ "$output" == *"sht-admin key add <user> <name> <public-key-file>"* ]]

  run "$BIN_ADMIN" authorized-keys
  [ "$status" -ne 0 ]
  [[ "$output" == *"sht-admin authorized-keys sync --output <path>"* ]]
}
