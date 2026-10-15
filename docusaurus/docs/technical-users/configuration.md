---
sidebar_position: 3
title: Complete Configuration Reference
description: Detailed schema and reference guide for config.yaml.
---

# ⚙️ Complete Configuration Reference (`config.yaml`)

TGDCFS is configured using a single YAML file located at `$TGDCFS_DATA_DIR/config.yaml`.

---

## Complete Example `config.yaml`

```yaml
backends:
  telegram:
    api_id: 123456
    api_hash: "0123456789abcdef0123456789abcdef"
    bot:
      session_file: bot.session
      tokens:
        - "123456789:ABCdefGHIjklMNOpqrsTUVwxyZ"
    delete_messages_on_remove: false

  discord:
    bot_tokens:
      - "BotTokenString1"
      - "BotTokenString2"
    max_file_size_bytes: 20000000  # 20 MB attachment limit
    delete_messages_on_remove: false

stores:
  tg-primary:
    backend: telegram
    channel: "-1001234567890"

  dc-mirror:
    backend: discord
    channel: "123456789012345678"

filesystems:
  media:
    primary: tg-primary
    mirrors:
      - dc-mirror
    mode: auto           # auto | forward | reupload
    sync: background     # inline | background | tee
    strict: false
    write_ack: primary   # primary | cache
    read_parallel: true
    read_sources:
      - tg-primary
    metadata:
      type: github_repo
      github_repo:
        repo: "your-username/tgdcfs-metadata"
        commit: "master"
        access_token: "ghp_yourPersonalAccessToken"

tgdcfs:
  users:
    admin:
      password: "SuperSecretPassword123"
      readonly: false
    guest:
      password: "GuestPassword123"
      readonly: true

  server:
    host: "0.0.0.0"
    port: 1900

  sftp:
    enabled: true
    host: "0.0.0.0"
    port: 2222
    host_key_file: "sftp_host_key"

  encryption:
    enabled: true
    encrypt_names: true
    passphrase_env: "TGDCFS_MASTER_PASSPHRASE"
    master_salt_file: "master.salt"
    chunk_size: 65536

  cache:
    enabled: true
    dir: "cache"
    max_size_mb: 20480
    stage_uploads: true
    keep_for_reads: true
    min_free_mb: 1024

  transfer:
    upload_workers_small: 3
    upload_workers_big: 8
    upload_part_size_kb: 512
    download_piece_size_kb: 4096
    download_pieces_in_flight: 4
```

---

## Configuration Section Details

### 1. `backends` Section
Configures authentication tokens and connection parameters for Telegram and Discord:

- **`telegram`**:
  - `api_id` *(int)*: Telegram API App ID from [my.telegram.org](https://my.telegram.org).
  - `api_hash` *(string)*: Telegram API App Hash.
  - `bot.tokens` *(list of strings)*: Telegram Bot tokens from [@BotFather](https://t.me/BotFather). Providing multiple tokens distributes download/upload operations across tokens.
- **`discord`**:
  - `bot_tokens` *(list of strings)*: Discord Bot tokens from the Discord Developer Portal.
  - `max_file_size_bytes` *(int)*: Maximum attachment size per uploaded message (default: `10000000` = 10 MB). Adjust based on your Discord server boost tier (Level 0: 20 MB, Level 2: 50 MB, Level 3: 100 MB).

---

### 2. `stores` Section
Defines logical store names mapped to channels:
- `backend` *(telegram | discord)*: Backend driver name.
- `channel` *(string)*: Channel ID (e.g. `-1001234567890` for Telegram or `123456789012345678` for Discord).

---

### 3. `filesystems` Section
Defines top-level virtual directories exposed via WebDAV/SFTP:
- `primary` *(string)*: Name of primary store defined in `stores`.
- `mirrors` *(list of strings)*: Names of mirror stores.
- `mode` *(auto | forward | reupload)*:
  - `auto`: Uses zero-bandwidth server-side copy (forwarding) when possible, falls back to reuploading.
  - `forward`: Requires server-side forwarding (Telegram to Telegram only).
  - `reupload`: Re-downloads and streams uploaded bytes to mirror store.
- `sync` *(inline | background | tee)*:
  - `inline`: Writes wait for primary and mirror operations to finish before responding to client.
  - `background`: Responds after primary write completes; queues mirror operations to background queue.
  - `tee`: Uploads to primary and reuploading mirrors simultaneously from incoming byte stream.
- `strict` *(bool)*: If `true`, fails client write operation if any mirror write fails.
- `write_ack` *(primary | cache)*:
  - `primary`: Client receives 201 Created after primary channel write finishes.
  - `cache`: Client receives 201 Created as soon as upload lands in local cache disk directory.

---

### 4. `tgdcfs` Section
Main application parameters:
- **`users`**: User accounts for WebDAV and SFTP authentication.
  - `readonly: true`: Users can list and download, but upload/delete operations return permission denied.
- **`sftp`**: Optional SFTP interface configuration.
- **`encryption`**: Client-side AES-256-GCM settings.
- **`cache`**: Local disk cache configuration.
- **`transfer`**: Fine-tuning piece size, concurrency workers, and buffers.
