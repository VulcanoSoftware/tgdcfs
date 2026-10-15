---
sidebar_position: 1
title: Technical Architecture Overview
description: High-level technical architecture of TGDCFS for sysadmins and self-hosters.
---

# 🛠️ Technical Architecture Overview

**TGDCFS** is a cloud filesystem bridge written in Python 3.13+ using **FastAPI**, **Uvicorn**, **Telethon**, **AsyncSSH**, and **discord.py**. It presents Telegram and Discord channels as unified **WebDAV** and **SFTP** storage backends.

---

## Core System Abstractions

TGDCFS divides storage into three fundamental abstractions: **Backends**, **Stores**, and **File Systems**.

```
+-------------------------------------------------------------------------+
|                            VIRTUAL LAYER                                |
|  WebDAV (/webdav)  |  SFTP (:2222)  |  Manager API  |  Telegram Mini App |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                        FILE SYSTEMS (Virtual Trees)                     |
|  File System "media"               |  File System "backups"             |
|   - Primary Store: tg-main         |   - Primary Store: dc-main         |
|   - Mirror Stores: [dc-mirror]     |   - Mirror Stores: [tg-backup]     |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           STORES (Channels)                             |
|  tg-main (Telegram)   |  dc-mirror (Discord)   |  dc-main (Discord)     |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           BACKEND DRIVERS                               |
|  Telegram (Telethon / MTProto)     |  Discord (discord.py / REST API)   |
+-------------------------------------------------------------------------+
```

### 1. Backends
A **Backend** is the underlying cloud protocol driver. TGDCFS supports two backends:
- **Telegram Backend**: Uploads chunked files up to 2 GiB per message (or 4 GiB with Premium) using MTProto RPC calls (`saveBigFilePart`).
- **Discord Backend**: Uploads files into attachments up to `max_file_size_bytes` (typically 10 MB - 100 MB depending on Discord server boost level) via HTTP REST endpoints.

### 2. Stores
A **Store** is a specific private channel on Telegram or Discord associated with a unique channel ID. Each store has a unique internal key format:
- Telegram Store Key: `tg:<channel_id>` (e.g., `tg:-1001234567890`)
- Discord Store Key: `dc:<channel_id>` (e.g., `dc:123456789012345678`)

### 3. File Systems
A **File System** represents a top-level directory exposed to WebDAV and SFTP users. Each file system consists of:
- **One Primary Store**: Where incoming new uploads write first.
- **Zero or More Mirror Stores**: Backup channels that receive copies or replicas of uploaded files.

---

## Metadata Strategies

TGDCFS maintains directory trees, file attributes, modification times, and chunk mapping in a **Metadata Repository**. You can choose between two metadata backends:

| Metadata Type | Description | Best Used For |
|---|---|---|
| `github_repo` | Stores encrypted directory metadata as JSON files in a private GitHub repository. | **Recommended for all production setups**, especially multi-backend setups (Discord + Telegram) or large trees. |
| `pinned_message` | Stores the metadata as a single pinned message attachment inside the store channel itself. | Small trees or setups where external GitHub tokens are not desired. |

---

## Core Feature Matrix

- 🔐 **At-Rest AES-256-GCM Encryption**: Client-side encryption before uploading to any cloud channel.
- 🎭 **Name Obfuscation**: Hides real directory/file names on Telegram/Discord channels and GitHub commits.
- 🔄 **Multi-Backend Mirroring**: Server-side forwarding between Telegram channels, or streaming re-uploads across Discord/Telegram.
- ⚡ **Local Disk Cache**: Staging uploads, read caching, and `write_ack: cache` write-back for fast client responses.
- 🚀 **Parallel Multi-Source Reads**: Downloads parts of a single file simultaneously from Telegram and Discord stores.
- 🔑 **Dual Protocol Interface**: Serves WebDAV and SFTP simultaneously from the same process engine.
