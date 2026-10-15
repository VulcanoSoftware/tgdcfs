---
sidebar_position: 2
title: Deployment & Self-Hosting Guide
description: How to deploy TGDCFS using Docker, Docker Compose, systemd, or bare metal.
---

# 🚀 Deployment & Self-Hosting Guide

TGDCFS can be deployed using **Docker**, **Docker Compose**, **systemd**, or directly on bare-metal Linux.

---

## Environment Variables

TGDCFS reads configuration and environment settings from standard environment variables:

| Variable | Default Value | Description |
|---|---|---|
| `TGDCFS_DATA_DIR` | `~/.tgdcfs` (`/home/tgdcfs/.tgdcfs` inside Docker) | Path to persistent data directory (holds sessions, host keys, queue files, cache). |
| `TGDCFS_CONFIG_FILE` | `$TGDCFS_DATA_DIR/config.yaml` | Path to the YAML configuration file. |
| `TGDCFS_MASTER_PASSPHRASE` | *(None)* | Secret passphrase used for deriving the AES-256-GCM master key when encryption is enabled. |
| `TGDCFS_MASTER_PASSPHRASE_FILE` | *(None)* | File path containing the passphrase (useful for systemd `LoadCredential`). |

*(Note: Legacy `TGFS_DATA_DIR` and `TGFS_CONFIG_FILE` environment variables are supported with a deprecation warning).*

---

## 1. Docker Deployment (Recommended)

Official Docker images are published on Docker Hub:
- Backend image: `xyvran/tgdcfs:latest`
- Frontend image: `xyvran/tgdcfs-fe:latest`

### Running with `docker run`

```bash
docker run -d \
  --name tgdcfs \
  --restart unless-stopped \
  -p 1900:1900 \
  -p 2222:2222 \
  -e TGDCFS_MASTER_PASSPHRASE="your-long-secret-passphrase" \
  -v ~/.tgdcfs:/home/tgdcfs/.tgdcfs \
  xyvran/tgdcfs:latest
```

---

## 2. Docker Compose Deployment

Here is a production-ready `docker-compose.yml` including both the backend server and frontend nginx container:

```yaml
version: "3.8"

services:
  tgdcfs:
    image: xyvran/tgdcfs:latest
    container_name: tgdcfs
    restart: unless-stopped
    ports:
      - "1900:1900"   # WebDAV & Manager API
      - "2222:2222"   # SFTP
    environment:
      - TGDCFS_MASTER_PASSPHRASE=${TGDCFS_MASTER_PASSPHRASE}
    volumes:
      - ./data:/home/tgdcfs/.tgdcfs

  frontend:
    image: xyvran/tgdcfs-fe:latest
    container_name: tgdcfs-fe
    restart: unless-stopped
    ports:
      - "8080:80"
    depends_on:
      - tgdcfs
```

Store your passphrase in a `.env` file alongside `docker-compose.yml`:
```env
TGDCFS_MASTER_PASSPHRASE=your-secure-random-passphrase
```

---

## 3. systemd Service Deployment

For native Linux host deployments, use systemd credentials for safe passphrase handling.

1. Create a service user and directory:
```bash
sudo useradd -m -s /bin/bash tgdcfs
sudo mkdir -p /etc/tgdcfs ~/.tgdcfs
```

2. Store passphrase securely in `/etc/tgdcfs/master.passphrase`:
```bash
echo "your-secure-passphrase" | sudo tee /etc/tgdcfs/master.passphrase
sudo chmod 0400 /etc/tgdcfs/master.passphrase
```

3. Create systemd unit file `/etc/systemd/system/tgdcfs.service`:
```ini
[Unit]
Description=TGDCFS Cloud Storage Bridge
After=network.target

[Service]
Type=simple
User=tgdcfs
WorkingDirectory=/home/tgdcfs
LoadCredential=master_passphrase:/etc/tgdcfs/master.passphrase
Environment=TGDCFS_MASTER_PASSPHRASE_FILE=%d/master_passphrase
ExecStart=/home/tgdcfs/.venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

4. Enable and start service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tgdcfs
```

---

## Data Directory Structure

Inside `$TGDCFS_DATA_DIR` (e.g. `/home/tgdcfs/.tgdcfs`), TGDCFS maintains:

```
.tgdcfs/
├── config.yaml               # Server configuration
├── bot.session               # Telethon Telegram bot session
├── master.salt               # 16-byte Argon2id salt (CRITICAL: DO NOT DELETE)
├── sftp_host_key             # Generated ed25519 SSH host key
├── sftp_authorized_keys/     # User public key files
├── replication_queue.json    # Persistent pending replication queue
├── cache/                    # Local cache files & block maps
└── cache/index.json          # Cache index database
```

> ⚠️ **CRITICAL BACKUP NOTICE**: Always back up `config.yaml`, `master.salt`, and `sftp_host_key`. Losing `master.salt` renders encrypted files permanently unrecoverable even with the correct passphrase!
