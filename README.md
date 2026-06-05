# sht

`sht` is a small BLAKE3 content-addressed blob store. The daemon serves an HTTP API over a Unix socket, and the shell wrapper lets SSH forced commands upload, fetch, and check blobs.

## Commands

- `shtd` runs the blob daemon.
- `sht-shell` is the SSH command wrapper.

Build and test:

```bash
make build
go test ./...
./tests/run.sh
```

## Runtime Configuration

`shtd` reads these environment variables:

- `SHT_SOCK_DIR`: Unix socket path. Defaults to `/run/sht/sht.sock`.
- `SHT_SOCK_MODE`: Unix socket permission mode. Defaults to `0660`.
- `SHT_SOCK_GROUP`: optional group name or numeric gid for the socket.
- `SHT_BLOB_DIR`: blob storage directory. Defaults to `/var/lib/sht/blobs`.
- `SHT_TMP_DIR`: temporary upload directory. Defaults to `/var/lib/sht/tmp`.
- `SHT_DB_PATH`: SQLite database path. Defaults to `/var/lib/sht/sht.db`.

The database contains `users`, `key_ids`, `shelves`, and `blob_refs`. Requests must include `X-SHT-Key-ID`, and the key ID must belong to an enabled key and enabled user.

Blobs are addressed by the BLAKE3 digest of the whole file. Physical storage is chunked under `SHT_BLOB_DIR/chunks`, and `blob_manifests` records which ordered chunk digests make up each final blob digest. `blob_refs` records which users can access each final blob digest. Each user has a `max_bytes` live quota. New users default to `3221225472` bytes, which is 3 GiB. The daemon enforces this quota from the sum of that user's clean `blob_refs`. Uploading content that a user already cleanly references does not consume quota again; uploading content already stored by another user creates a new reference and counts against the new user's live quota.

Users also have a per-GC-cycle pending quota. New users default to `4026531840` pending bytes, which is 1.25x the default live quota. Creating new physical chunks increments `users.pending_bytes`; referencing already-stored global chunks does not. Releasing a blob marks the user's `blob_refs` row dirty and frees live quota, but it does not decrement pending bytes. Dirty refs are listed for visibility, cannot be fetched, and can be restored by uploading the same content again. A future GC pass should delete physical chunks with no clean refs, delete their dirty ref rows, and reset `pending_bytes` on users and shelves; tests simulate that manually.

Every user starts with a default shelf initially named `main`. The default shelf is a role, so it can be renamed or moved to another shelf. A shelf is a per-user quota partition for app-level ownership. Creating any non-default shelf enables multi-shelf mode for that user. After that, scoped operations must name a shelf with `X-SHT-Shelf`; unscoped `GET /blob/<digest>` and `HEAD /blob/<digest>` still succeed if the user has any clean shelf ref for the digest. User live quota counts each clean digest once across all shelves, while shelf live quota counts refs inside that shelf. Shelf pending quota is enforced independently for new physical bytes created through that shelf.

Simple `POST /blob` uploads remain supported, but each user has a `max_simple_upload_bytes` cutoff. New users default to `67108864` bytes, which is 64 MiB. Larger uploads should use the resumable manifest workflow: `POST /uploads`, `PUT /uploads/<digest>/chunks/<index>`, `GET /uploads/<digest>`, and `POST /uploads/<digest>/finalize`.

`GET /quota` returns total user quota usage: live bytes, pending bytes, bytes reserved by open upload sessions, and clean digest count. Shelf management uses `GET /shelves` and `POST /shelves` with JSON like `{"name":"app1","max_bytes":3221225472,"max_pending_bytes":4026531840}`. Shelf responses include `used_bytes`, `pending_bytes`, and `ref_count` for quota visibility. `PATCH /shelves/<name>` with `{"name":"new-name"}` renames a shelf, and `POST /shelves/<name>/default` makes a shelf the default. `DELETE /shelves/<name>?force=1` hard-deletes a non-default shelf's refs, upload sessions, and shelf row; physical blobs are left for GC.

Older test blobs stored under per-key directories are not migrated into `blob_refs`. For a clean test/dev reset, stop `shtd`, delete `SHT_BLOB_DIR`, recreate it with the daemon user's ownership, and restart the daemon.

## Socket Access

Direct local clients can talk to `shtd` through the Unix socket. The recommended setup is to gate that socket with a Unix group:

```bash
sudo groupadd --system sht
sudo usermod -aG sht some-app-user
```

Run `shtd` with:

```bash
SHT_SOCK_GROUP=sht
SHT_SOCK_MODE=0660
```

Members of that group are trusted local clients. They can connect directly to the socket and choose any enabled `X-SHT-Key-ID`, so only add trusted app users to the group. Stronger per-app isolation would need peer credential checks, separate sockets, or another capability mechanism.

Example direct upload:

```bash
printf 'hello sht' | curl --unix-socket /run/sht/sht.sock \
  -H 'X-SHT-Key-ID: 1' \
  -X POST http://sht/blob \
  --data-binary @-
```

## SSH Usage

Use `sht-shell` as a per-key forced command in `authorized_keys`:

```text
command="sht-shell id 1" ssh-ed25519 AAAA...
```

Do not use a global `ForceCommand` for this wrapper. The wrapper expects the key ID in its own command arguments and reads the user's requested command from `SSH_ORIGINAL_COMMAND`.

Supported SSH commands. Add `-j` anywhere after the remote command to force JSON output, for example `sht -- -j`, `sht list -j`, or `sht shelf list -j`.

```bash
sht                       # upload stdin to default shelf
sht <shelf>               # upload stdin to shelf
sht --                    # upload stdin to default shelf
sht <shelf> --            # upload stdin to shelf
sht <command> [args...]

sht cat [digest ...]      # print blobs
sht stat [digest ...]     # show blob status
sht release [digest ...]  # release blobs
sht list [fields]         # list refs
sht quota                 # show quota usage
sht alias ns list         # list alias namespaces
sht alias ns create <name>
sht alias list <namespace> [prefix]
sht alias get <namespace> <path>
sht alias cat <namespace> <path>
sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]
sht alias history <namespace> <path>
sht alias grant <namespace> <path> <user> <read|write|admin>
sht alias revoke <namespace> <path> <user>
sht shelf list
sht shelf create <name> <max> [pending-max]
sht shelf rename <old> <new>
sht shelf default <name>
sht shelf delete <name> --force
sht manifest              # create/resume upload from manifest JSON
sht upload <id> <index>   # upload chunk bytes
sht status <id>           # show upload status
sht finalize <id>         # finalize upload
sht help [topic]          # show usage
```

Digest commands read whitespace-delimited digests from stdin when none are given. `sht help list`, `sht help shelf`, `sht help alias`, and `sht help upload` show detailed topic help.

Alias namespaces are shared spaces for versioned names that point at blobs. Grants are recursive by alias path, so granting `write` on `docs` lets that user update aliases under `docs/...`. Updating an existing alias should pass `--expect <version>` to avoid overwriting someone else's newer commit.
