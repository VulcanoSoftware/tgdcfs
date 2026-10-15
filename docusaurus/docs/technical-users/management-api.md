---
sidebar_position: 7
title: Management API & Monitoring
description: REST endpoints reference for system status, stores, cache, and replication management.
---

# 📡 Management API & Monitoring

TGDCFS provides a RESTful **Manager API** served on port `1900` under `/api`.

---

## Authentication

When `tgdcfs.jwt` authentication is enabled, requests require a Bearer token in the `Authorization` header:

```http
Authorization: Bearer <jwt_token>
```

---

## Endpoint Reference

### 1. Store & Filesystem Information

#### `GET /api/stores`
Returns list of configured stores, backends, channels, and health status.

**Example Response**:
```json
{
  "stores": [
    {
      "key": "tg:-1001234567890",
      "name": "tg-primary",
      "backend": "telegram",
      "channel": "-1001234567890",
      "dead": false,
      "caps": {
        "max_part_bytes": 2147483648,
        "supports_server_copy": true
      }
    }
  ]
}
```

#### `GET /api/filesystems`
Returns list of configured file systems, primary/mirror stores, sync mode, and settings.

**Example Response**:
```json
{
  "filesystems": [
    {
      "name": "media",
      "primary": "tg-primary",
      "mirrors": ["dc-mirror"],
      "mode": "auto",
      "sync": "background",
      "write_ack": "primary",
      "read_parallel": true
    }
  ]
}
```

---

### 2. Cache Operations

#### `GET /api/cache`
Reports local disk cache status, disk space, entry count, pinned bytes, and hit ratio.

**Example Response**:
```json
{
  "enabled": true,
  "bytes_used": 1073741824,
  "max_size_mb": 20480,
  "entries_count": 42,
  "pinned_bytes": 104857600,
  "free_disk_mb": 154200,
  "hit_ratio": 0.88
}
```

#### `POST /api/cache/evict`
Triggers an immediate cache eviction pass, dropping unpinned read-cache entries down to target fill percent.

---

### 3. Replication & Tasks

#### `GET /api/replication/queue`
Returns pending background replication queue items.

#### `POST /api/replication/retry`
Immediately retries all failed replication items in the queue.

#### `POST /api/redundancy/backfill/{filesystem}`
Triggers a full scan and backfill task for the specified file system. Pass `?verify=true` to re-verify existing copies.

#### `GET /api/tasks`
Returns real-time status of active and historical background tasks (backfill, distribution, cache sweep).

---

## Health Monitoring

Use `/api/stores` or `/api/filesystems` for automated health probes in Kubernetes or Docker health checks:

```bash
curl -f http://localhost:1900/api/stores || exit 1
```
