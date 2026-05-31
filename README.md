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

The database contains `users`, `key_ids`, and `blob_refs`. Requests must include `X-SHT-Key-ID`, and the key ID must belong to an enabled key and enabled user.

Blobs are stored globally by digest under `SHT_BLOB_DIR`, while `blob_refs` records which users can access each digest. Each user has a `max_bytes` live quota. New users default to `3221225472` bytes, which is 3 GiB. The daemon enforces this quota from the sum of that user's `blob_refs`. Uploading content that a user already references does not consume quota again; uploading content already stored by another user creates a new reference and counts against the new user's live quota.

Users also have a per-GC-cycle pending quota. New users default to `4026531840` pending bytes, which is 1.25x the default live quota. Creating a new physical blob increments `users.pending_bytes`; referencing an already-stored global blob does not. Releasing a blob removes the user's `blob_refs` row and frees live quota, but it does not decrement pending bytes. A future GC pass should delete unreferenced physical blobs and reset `pending_bytes`; tests simulate that manually.

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

Supported SSH commands:

```bash
sht               # upload stdin
sht cat <digest>      # print blob
sht stat <digest>     # check blob exists
sht release <digest>  # release blob access
sht help          # show usage
```
