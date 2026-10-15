---
sidebar_position: 5
title: Redundancy, Mirroring & Disaster Recovery
description: Multi-channel RAID-1 style store redundancy, sync modes, replicas, and promotion runbook.
---

# 🔄 Redundancy, Mirroring & Disaster Recovery

TGDCFS provides RAID-1 style channel redundancy. A single **File System** can mirror data across multiple Telegram and Discord channels.

---

## Aligned Copies vs Replicas

When mirroring files between stores, TGDCFS handles differences in part limits automatically:

- **Aligned Copies**: Used when primary and mirror stores have identical part size limits (e.g., Telegram primary to Telegram mirror). Every message in the primary store maps 1-to-1 with a message in the mirror store (`mirrors` map).
- **Replicas**: Used when primary and mirror stores have different part size limits (e.g., a 2 GiB Telegram primary mirrored to a 20 MB Discord channel). TGDCFS cuts the file into Discord-sized pieces and tracks them in a compact `replicas` map.

---

## Replication Copy Modes (`mode`)

Configured per file system under `filesystems.<fs>.mode`:

| Mode | Behaviour | Bandwidth Cost |
|---|---|---|
| `auto` *(default)* | Attempts server-side message forwarding (`copy_from`). Falls back to streaming reupload if forwarding is unsupported or blocked. | Zero bandwidth for Telegram-to-Telegram; streaming for cross-backend. |
| `forward` | Forces server-side message forwarding (`forwardMessages`). Fails if channel has "Restrict saving content". | Zero bandwidth. |
| `reupload` | Always re-downloads and streams uploaded bytes to target mirror. | Requires double bandwidth. |

---

## Replication Sync Modes (`sync`)

Configured per file system under `filesystems.<fs>.sync`:

### 1. `inline`
- Client PUT request blocks until upload completes on primary store AND all mirror stores.
- Required if `strict: true` is set.

### 2. `background` *(default for cross-backend/reupload)*
- Client PUT request receives `201 Created` as soon as primary store write succeeds.
- Reupload tasks are added to the persistent background queue (`replication_queue.json`) and processed asynchronously by background worker threads.

### 3. `tee`
- Bytes read from the incoming client stream are duplicated in memory to bounded branch buffers (`tee_buffer_mb`, default 64 MB).
- Uploads to primary and reuploading mirrors run concurrently in parallel during the client PUT request. No local disk cache required.

---

## Failover Reads

If a message chunk or store becomes unreachable (e.g. FloodWait rate limit or network issue):
- TGDCFS automatically fails over to available mirror stores or replicas chunk-by-chunk.
- Read operations resume seamlessly at the exact byte where the failure occurred.

---

## Disaster Recovery / Promotion Runbook

If a primary store channel is banned, deleted, or permanently lost, follow this step-by-step promotion runbook to swap a mirror store into the primary role:

### Step 1: Stop TGDCFS Service
```bash
docker stop tgdcfs  # or sudo systemctl stop tgdcfs
```

### Step 2: Edit `config.yaml`
In `config.yaml`, swap store roles under `filesystems`:

```yaml
# BEFORE
filesystems:
  media:
    primary: tg-main
    mirrors:
      - tg-backup

# AFTER
filesystems:
  media:
    primary: tg-backup
    mirrors: []  # or keep tg-main if temporarily disabled
```

### Step 3: Restart TGDCFS
```bash
docker start tgdcfs  # or sudo systemctl start tgdcfs
```
*Note: TGDCFS relocates existing store keys on boot. Files remain fully readable from `tg-backup` immediately.*

### Step 4: Execute Backfill Task
Trigger backfill via curl to ensure all version descriptors are updated and copies are promoted:

```bash
curl -X POST http://localhost:1900/api/redundancy/backfill/media
```

Check task progress:
```bash
curl http://localhost:1900/api/tasks
```

### Step 5: Restore Redundancy
Add a fresh backup channel to `mirrors` in `config.yaml` and run backfill once more to re-establish RAID-1 redundancy!
