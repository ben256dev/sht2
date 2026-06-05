# SHT Identity and Gateway TODO

## 1. DB-backed SSH keys

- Store SSH public keys in `key_ids`.
- Keep key labels, enabled/disabled state, user ownership, and timestamps.
- Treat the DB as the source of truth for SSH access.
- Generate forced-command `authorized_keys` entries from enabled DB rows.
- Regenerate `authorized_keys` atomically when key state changes.
- Provide a manual sync command to repair drift between DB state and `authorized_keys`.

## 2. Admin key utility

- Add a local admin command for creating users.
- Add commands for adding, disabling, and listing SSH keys.
- Add a command to regenerate/sync `authorized_keys`.
- Make the sync operation atomic so partial writes do not break SSH login.

## 3. SHT Gateway

- Add an HTTP service behind a reverse proxy.
- Authenticate users through Keycloak/OIDC.
- Map Keycloak users to `shtd` users.
- Allow authenticated users to upload/register SSH public keys.
- Forward future browser/client operations to `shtd` over the Unix socket.

## 4. User model

- Treat internal workers as normal `shtd` users.
- Treat shthub users as normal `shtd` users mapped from Keycloak subjects.
- Keep `shthub` as a possible service account, not the owner of every external user's state.
- Use shelves for quota/storage partitioning, not identity.

## 5. Blob and metadata access model

- Blob bytes are public by digest.
- Blob metadata remains private or permissioned.
- Aliases, refs, shelves, quota, and history are authenticated resources.
- Shthub may serve `/blob/<digest>` publicly through the gateway.

## 6. Browser uploads

- Reuse existing simple and resumable upload APIs through the gateway.
- Make browser uploads act as the mapped `shtd` user.
- Preserve existing quota, shelf, and alias behavior.
