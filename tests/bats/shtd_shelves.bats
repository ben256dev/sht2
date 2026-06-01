#!/usr/bin/env bats

load 'helper.bash'

setup() {
  setup_test_env
}

teardown() {
  teardown_test_env
}

create_shelf() {
  local name="$1"
  local max_bytes="$2"
  local max_pending_bytes="$3"
  curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/shelves \
    --data-binary '{"name":"'"$name"'","max_bytes":'"$max_bytes"',"max_pending_bytes":'"$max_pending_bytes"'}'
}

upload_code() {
  local shelf="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS -o "$TEST_TMPDIR/upload.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H "X-SHT-Shelf: $shelf" \
    -X POST http://sht/blob \
    --data-binary @-
}

upload_digest() {
  local shelf="$1"
  local payload="$2"
  printf '%s' "$payload" | curl -sS \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H "X-SHT-Shelf: $shelf" \
    -X POST http://sht/blob \
    --data-binary @- \
    | sed -n 's/.*"digest":"\([^"]*\)".*/\1/p'
}

seed_old_schema_db() {
  rm -f "$SHT_DB_PATH"
  sqlite3 "$SHT_DB_PATH" <<'SQL'
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE key_ids (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE blob_refs (
  user_id INTEGER NOT NULL,
  key_id INTEGER NOT NULL,
  digest TEXT NOT NULL,
  size INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  dirty INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, digest)
);
INSERT INTO users (id, name, enabled) VALUES (1, 'test-user-1', 1);
INSERT INTO key_ids (id, user_id, name, enabled) VALUES (1, 1, 'test-key-1', 1);
INSERT INTO blob_refs (user_id, key_id, digest, size, dirty) VALUES (1, 1, 'old-ref-digest', 7, 0);
SQL
}

@test "shtd shelves list default and require shelf after multi-shelf is enabled" {
  start_shtd

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"default"'* ]]

  created="$(create_shelf app1 3 10)"
  [[ "$created" == *'"name":"app1"'* ]]

  code="$(printf 'xyz' | curl -sS -o "$TEST_TMPDIR/no-shelf.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "400" ]
  grep -q 'shelf required' "$TEST_TMPDIR/no-shelf.out"
  grep -q 'sht shelf list' "$TEST_TMPDIR/no-shelf.out"
  grep -q 'X-SHT-Shelf' "$TEST_TMPDIR/no-shelf.out"

  [ "$(upload_code app1 abc)" = "200" ]
  digest="$(sed -n 's/.*"digest":"\([^"]*\)".*/\1/p' "$TEST_TMPDIR/upload.out")"
  [ -n "$digest" ]

  duplicate="$(printf 'abc' | curl -sS \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [[ "$duplicate" == *'"digest":"'"$digest"'"'* ]]
  [[ "$duplicate" == *'"exists":true'* ]]

  refs_code="$(curl -sS -o "$TEST_TMPDIR/refs.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    http://sht/refs)"
  [ "$refs_code" = "200" ]
  grep -q "$digest" "$TEST_TMPDIR/refs.out"
  grep -q '"shelf":"app1"' "$TEST_TMPDIR/refs.out"

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: app1' http://sht/refs)"
  [[ "$refs" == *"$digest"* ]]
  [[ "$refs" == *'"shelf":"app1"'* ]]

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"app1"'* ]]
  [[ "$shelves" == *'"used_bytes":3'* ]]
  [[ "$shelves" == *'"pending_bytes":3'* ]]
  [[ "$shelves" == *'"ref_count":1'* ]]
}

@test "shtd refs can list all shelves or one shelf" {
  start_shtd
  create_shelf app1 20 20 >/dev/null
  create_shelf app2 20 20 >/dev/null

  d1="$(upload_digest app1 one)"
  d2="$(upload_digest app2 two)"
  [ -n "$d1" ]
  [ -n "$d2" ]

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  [[ "$refs" == *"$d1"* ]]
  [[ "$refs" == *'"shelf":"app1"'* ]]
  [[ "$refs" == *"$d2"* ]]
  [[ "$refs" == *'"shelf":"app2"'* ]]

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: app1' http://sht/refs)"
  [[ "$refs" == *"$d1"* ]]
  [[ "$refs" != *"$d2"* ]]
}

@test "shtd user quota counts a digest once across shelves" {
  sqlite3 "$SHT_DB_PATH" "UPDATE users SET max_bytes = 3, max_pending_bytes = 20 WHERE id = 1"
  start_shtd
  create_shelf a 3 20 >/dev/null
  create_shelf b 4 20 >/dev/null

  [ "$(upload_code a abc)" = "200" ]
  [ "$(upload_code b abc)" = "200" ]
  [ "$(upload_code b d)" = "413" ]
  grep -q 'quota exceeded' "$TEST_TMPDIR/upload.out"

  quota="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/quota)"
  [[ "$quota" == *'"used_bytes":3'* ]]
  [[ "$quota" == *'"clean_digest_count":1'* ]]

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"a","max_bytes":3,"used_bytes":3'* ]]
  [[ "$shelves" == *'"name":"b","max_bytes":4,"used_bytes":3'* ]]
}

@test "shtd shelf pending quota only charges new physical content" {
  start_shtd
  create_shelf a 20 20 >/dev/null
  create_shelf b 20 20 >/dev/null

  [ "$(upload_code a abc)" = "200" ]
  [ "$(upload_code b abc)" = "200" ]

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"a","max_bytes":20,"used_bytes":3,"max_pending_bytes":20,"pending_bytes":3'* ]]
  [[ "$shelves" == *'"name":"b","max_bytes":20,"used_bytes":3,"max_pending_bytes":20,"pending_bytes":0'* ]]
}

@test "shtd release is scoped to a shelf while fetch can use any clean shelf" {
  start_shtd
  create_shelf a 20 20 >/dev/null
  create_shelf b 20 20 >/dev/null

  digest="$(upload_digest a shared)"
  [ -n "$digest" ]
  [ "$(upload_code b shared)" = "200" ]

  release_code="$(curl -sS -o "$TEST_TMPDIR/release.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'X-SHT-Shelf: a' \
    -X DELETE "http://sht/blob/$digest")"
  [ "$release_code" = "204" ]

  body="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' "http://sht/blob/$digest")"
  [ "$body" = "shared" ]

  code_a="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: a' "http://sht/blob/$digest")"
  code_b="$(curl -sS -o /dev/null -w "%{http_code}" --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: b' "http://sht/blob/$digest")"
  [ "$code_a" = "404" ]
  [ "$code_b" = "200" ]

  refs_a="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: a' http://sht/refs)"
  refs_b="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -H 'X-SHT-Shelf: b' http://sht/refs)"
  [[ "$refs_a" == *'"dirty":true'* ]]
  [[ "$refs_b" == *'"dirty":false'* ]]
}

@test "shtd shelf delete requires force and removes shelf" {
  start_shtd
  create_shelf doomed 20 20 >/dev/null

  digest="$(upload_digest doomed payload)"
  [ -n "$digest" ]

  code="$(curl -sS -o "$TEST_TMPDIR/delete.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X DELETE http://sht/shelves/doomed)"
  [ "$code" = "400" ]
  grep -q 'force required' "$TEST_TMPDIR/delete.out"

  code="$(curl -sS -o "$TEST_TMPDIR/delete-default.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X DELETE 'http://sht/shelves/default?force=1')"
  [ "$code" = "400" ]
  grep -q 'cannot delete default shelf' "$TEST_TMPDIR/delete-default.out"

  code="$(curl -sS -o "$TEST_TMPDIR/delete-force.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X DELETE 'http://sht/shelves/doomed?force=1')"
  [ "$code" = "204" ]

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" != *'"name":"doomed"'* ]]

  code="$(curl -sS -o "$TEST_TMPDIR/refs-disabled.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'X-SHT-Shelf: doomed' \
    http://sht/refs)"
  [ "$code" = "400" ]
  grep -q 'shelf not found' "$TEST_TMPDIR/refs-disabled.out"
  grep -q 'sht shelf list' "$TEST_TMPDIR/refs-disabled.out"
  grep -q 'sht shelf create doomed <max_bytes> <max_pending_bytes>' "$TEST_TMPDIR/refs-disabled.out"

  code="$(curl -sS -o /dev/null -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    "http://sht/blob/$digest")"
  [ "$code" = "404" ]

  quota="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/quota)"
  [[ "$quota" == *'"used_bytes":0'* ]]

  ref_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM blob_refs WHERE digest = '$digest'")"
  [ "$ref_count" = "0" ]

  create_shelf doomed 20 20 >/dev/null
  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"doomed"'* ]]
}

@test "shtd deleting last non-default shelf disables multi-shelf mode" {
  start_shtd
  create_shelf solo 20 20 >/dev/null

  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X DELETE 'http://sht/shelves/solo?force=1'

  code="$(printf 'abc' | curl -sS -o "$TEST_TMPDIR/upload-default.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "200" ]
}

@test "shtd rejects reserved and invalid shelf names" {
  start_shtd

  code="$(curl -sS -o "$TEST_TMPDIR/create-quota.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/shelves \
    --data-binary '{"name":"quota","max_bytes":20,"max_pending_bytes":20}')"
  [ "$code" = "400" ]
  grep -q 'reserved shelf name' "$TEST_TMPDIR/create-quota.out"

  code="$(curl -sS -o "$TEST_TMPDIR/create-list.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/shelves \
    --data-binary '{"name":"list","max_bytes":20,"max_pending_bytes":20}')"
  [ "$code" = "400" ]
  grep -q 'reserved shelf name' "$TEST_TMPDIR/create-list.out"

  code="$(curl -sS -o "$TEST_TMPDIR/create-invalid.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X POST http://sht/shelves \
    --data-binary '{"name":"bad/name","max_bytes":20,"max_pending_bytes":20}')"
  [ "$code" = "400" ]
  grep -q 'invalid shelf name' "$TEST_TMPDIR/create-invalid.out"
}

@test "shtd can rename shelves and change default shelf" {
  start_shtd

  renamed="$(curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X PATCH http://sht/shelves/default \
    --data-binary '{"name":"main"}')"
  [[ "$renamed" == *'"name":"main"'* ]]
  [[ "$renamed" == *'"is_default":true'* ]]

  code="$(printf 'plain' | curl -sS -o "$TEST_TMPDIR/plain.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "200" ]

  create_shelf app1 20 20 >/dev/null
  digest="$(upload_digest app1 scoped)"
  [ -n "$digest" ]

  changed="$(curl -sS --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/shelves/app1/default)"
  [[ "$changed" == *'"name":"app1"'* ]]
  [[ "$changed" == *'"is_default":true'* ]]

  code="$(curl -sS -o "$TEST_TMPDIR/delete-new-default.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X DELETE 'http://sht/shelves/app1?force=1')"
  [ "$code" = "400" ]
  grep -q 'cannot delete default shelf' "$TEST_TMPDIR/delete-new-default.out"

  code="$(curl -sS -o "$TEST_TMPDIR/rename-existing.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X PATCH http://sht/shelves/main \
    --data-binary '{"name":"app1"}')"
  [ "$code" = "409" ]

  code="$(curl -sS -o "$TEST_TMPDIR/rename-default.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X PATCH http://sht/shelves/main \
    --data-binary '{"name":"default"}')"
  [ "$code" = "200" ]
  grep -q '"name":"default"' "$TEST_TMPDIR/rename-default.out"
  grep -q '"is_default":false' "$TEST_TMPDIR/rename-default.out"
}

@test "shtd unscoped upload remains disallowed after default role moves" {
  start_shtd

  code="$(curl -sS -o "$TEST_TMPDIR/rename.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -H 'Content-Type: application/json' \
    -X PATCH http://sht/shelves/default \
    --data-binary '{"name":"main"}')"
  [ "$code" = "200" ]

  code="$(printf 'plain' | curl -sS -o "$TEST_TMPDIR/plain.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "200" ]

  create_shelf app1 20 20 >/dev/null
  create_shelf app2 20 20 >/dev/null
  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/shelves/app1/default
  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X DELETE 'http://sht/shelves/main?force=1'

  code="$(printf 'blocked' | curl -sS -o "$TEST_TMPDIR/blocked.out" -w "%{http_code}" \
    --unix-socket "$SHT_SOCK_DIR" \
    -H 'X-SHT-Key-ID: 1' \
    -X POST http://sht/blob \
    --data-binary @-)"
  [ "$code" = "400" ]
  grep -q 'shelf required' "$TEST_TMPDIR/blocked.out"
  grep -q 'sht shelf list' "$TEST_TMPDIR/blocked.out"
}

@test "shtd can recreate a shelf named default after default role moves" {
  start_shtd

  create_shelf app1 20 20 >/dev/null
  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X POST http://sht/shelves/app1/default
  curl -sS -o /dev/null --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' -X DELETE 'http://sht/shelves/default?force=1'

  created="$(create_shelf default 20 20)"
  [[ "$created" == *'"name":"default"'* ]]
  [[ "$created" == *'"is_default":false'* ]]
}

@test "shtd migrates old refs onto a default shelf" {
  seed_old_schema_db
  start_shtd

  shelves="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/shelves)"
  [[ "$shelves" == *'"name":"default"'* ]]
  [[ "$shelves" == *'"is_default":true'* ]]

  refs="$(curl -sS --unix-socket "$SHT_SOCK_DIR" -H 'X-SHT-Key-ID: 1' http://sht/refs)"
  [[ "$refs" == *'"digest":"old-ref-digest"'* ]]
  [[ "$refs" == *'"shelf":"default"'* ]]

  primary_key_count="$(sqlite3 "$SHT_DB_PATH" "SELECT COUNT(*) FROM pragma_table_info('blob_refs') WHERE name = 'shelf_id'")"
  [ "$primary_key_count" = "1" ]
}
