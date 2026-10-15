---
sidebar_position: 1
title: Developer Overview & Architecture Tour
description: Deep-dive architecture tour of the TGDCFS codebase for developers and contributors.
---

# 💻 Developer Overview & Architecture Tour

Welcome to the **TGDCFS Developer Guide**! This documentation is written for software engineers who want to understand the inner workings of TGDCFS, add new features, fix bugs, or contribute to the project.

---

## Technical Stack Summary

- **Language**: Python 3.13+ (strict type annotations with `mypy`)
- **Package Manager**: [Poetry](https://python-poetry.org/)
- **Web & Async Framework**: [FastAPI](https://fastapi.tiangolo.com/), [Uvicorn](https://www.uvicorn.org/), `uvloop` (on Linux)
- **Protocols**:
  - **Telegram**: [Telethon](https://github.com/LonamiWebs/Telethon) (MTProto)
  - **Discord**: [discord.py](https://github.com/Rapptz/discord.py)
  - **SFTP**: [AsyncSSH](https://asyncssh.readthedocs.io/)
  - **WebDAV**: Custom ASGI WebDAV pipeline (`asgidav/`)
- **Testing & Quality Assurance**: `pytest`, `pytest-asyncio`, `mypy`, `ruff`, `pre-commit`

---

## Codebase Directory Structure

```
tgdcfs/
├── main.py                     # Server entrypoint (Uvicorn + FastAPI + SFTP initialization)
├── config-test.yaml            # Test configuration file
├── demo-config.yaml            # Full annotated example config
├── pyproject.toml              # Dependencies, build metadata, tool settings
├── makefile                    # Quick development commands (make mypy, make ruff)
├── tgdcfs/                     # Main Python package
│   ├── config.py               # Config loading, schema v2, legacy tgfs translation
│   ├── backends/               # Backend drivers & store abstractions
│   │   ├── base.py             # IStore interface & StoreCapabilities
│   │   ├── telegram/           # TelegramStore implementation (Telethon)
│   │   └── discord/            # DiscordStore implementation (discord.py)
│   ├── core/                   # Core filesystem domain logic
│   │   ├── store.py            # Store wrapper
│   │   ├── local_cache.py      # LocalCache, StagingWriter, VersionBytes
│   │   ├── multisource.py      # MultiSourceRead parallel scheduler
│   │   ├── replication.py      # Persistent ReplicationQueue & workers
│   │   ├── backfill.py         # Store backfill & promotion logic
│   │   ├── mirror.py           # MirrorGroup & replication dispatch
│   │   ├── repository/         # File, FileDesc, Metadata repositories
│   │   └── model/              # FileVersion, FileRef, Replica data models
│   ├── crypto/                 # AES-256-GCM encryption & Argon2id key derivation
│   ├── app/                    # Protocol servers & web endpoints
│   │   ├── webdav/             # WebDAV request router & provider
│   │   ├── sftp/               # AsyncSSH SFTP server & user handling
│   │   └── manager/            # Manager REST API endpoints (/api/*)
│   └── tasks/                  # Task store & async task execution runner
├── tests/                      # Full test suite
│   ├── fakes/                  # In-memory fake stores for offline testing
│   ├── test_cache.py           # Cache unit tests
│   ├── test_replication.py     # Replication engine unit tests
│   └── ...                     # 57+ unit test modules
├── scripts/                    # Helper scripts
│   ├── docker_smoke_check.py   # Container smoke test script
│   └── measure_transfer.py     # Download piece transfer benchmarking tool
├── tgdcfs-gh-pages/            # Next.js web frontend & Telegram Mini App
└── docusaurus/                 # Docusaurus documentation website
```

---

## Key Core Interfaces & Components

### 1. `IStore` Interface (`tgdcfs/backends/base.py`)
`IStore` is the foundational abstraction that decouples repository logic from specific cloud backends:

```python
class IStore(Protocol):
    key: str
    caps: StoreCapabilities

    async def send_text(self, text: str) -> int: ...
    async def get_text(self, message_id: int) -> Optional[str]: ...
    async def upload(self, file_msg: UploadableFileMessage) -> list[SentFileMessage]: ...
    async def download(self, message_id: int, begin: int, end: int) -> DownloadFileResp: ...
    async def copy_within(self, ids: list[int]) -> list[int]: ...
    async def copy_from(self, other: "IStore", ids: list[int]) -> Optional[list[int]]: ...
```

### 2. `StoreFileContentRepository` (`tgdcfs/core/repository/`)
Handles raw byte stream operations, chunking, range slicing, local cache integration, and multi-source read dispatch.

### 3. `ReplicationQueue` & `ReplicationWorker` (`tgdcfs/core/replication.py`)
Provides persistent asynchronous file replication across Telegram and Discord stores, persisting queue items to `replication_queue.json`.

---

## Next Steps for Developers

- 🛠️ [Local Repository Setup](./local-setup.md) – How to clone, set up Python 3.13+, Poetry, git hooks, and build/test Docker containers locally.
- 🧪 [Testing & QA Guide](./testing-and-qa.md) – Running pytest, typechecking, linting, and offline fake store tests.
- 🤝 [Contributing Guidelines](./contributing.md) – GitHub fork setup, repository secrets, CI/CD workflow guide, commit conventions, and releases.
