---
sidebar_position: 3
title: Local Testing & Quality Assurance
description: Guide to running unit tests, static typechecking, linting, and smoke tests locally.
---

# 🧪 Local Testing & Quality Assurance

TGDCFS maintains high test coverage and strict type safety. All code changes must pass unit tests, type checks, and linter rules before being merged.

---

## Testing Pipeline Overview

Commands to run before submitting any code:

```bash
# 1. Run unit tests
poetry run pytest

# 2. Run static type checker
poetry run mypy .

# 3. Run linter and code style checks
poetry run ruff check .
```

---

## 1. Running Unit Tests with `pytest`

The test suite contains over 57 test modules covering all subsystems (`tests/`).

### Basic Test Invocations

Run the complete test suite:
```bash
poetry run pytest
```

Run a specific test file:
```bash
poetry run pytest tests/test_cache.py
```

Run a specific test function with verbose output:
```bash
poetry run pytest tests/test_replication.py -k "test_queue_retry" -v
```

Generate test coverage report:
```bash
poetry run pytest --cov=tgdcfs --cov-report=term-missing
```

---

## 2. Testing with In-Memory Fake Stores (`tests/fakes/store.py`)

You **do not need live Telegram or Discord API tokens** to write or run unit tests!

TGDCFS provides a feature-rich in-memory fake store implementation in `tests/fakes/store.py`:
- `FakeStore`: Implements `IStore` entirely in memory.
- Configurable capabilities (`max_part_bytes`, `max_text_chars`, `supports_server_copy`).
- Fault injection: Simulate network timeouts, `FloodWait` rate limits, transient channel errors, or dead stores.

Example unit test using `FakeStore`:

```python
import pytest
from tests.fakes.store import FakeStore
from tgdcfs.backends.base import StoreCapabilities

@pytest.mark.asyncio
async def test_fake_store_upload_and_download():
    # Construct a fake store simulating a Discord channel (10 MB parts)
    caps = StoreCapabilities(
        max_part_bytes=10_000_000,
        max_text_chars=2000,
        supports_server_copy=False,
        supports_edit_media=True,
        supports_bulk_delete=True,
        delete_age_limit_days=14,
    )
    store = FakeStore(key="dc:123456", caps=caps)

    # Test uploading text and binary data
    msg_id = await store.send_text("Hello TGDCFS")
    retrieved_text = await store.get_text(msg_id)
    assert retrieved_text == "Hello TGDCFS"
```

---

## 3. Static Typechecking with `mypy`

TGDCFS uses strict `mypy` configuration for full static type coverage:

```bash
poetry run mypy .
# or use the Makefile shortcut
make mypy
```

Ensure no type errors (`Success: no issues found in XX source files`) remain.

---

## 4. Linting & Code Style with `ruff`

TGDCFS uses **Ruff** for fast linting and code formatting:

```bash
# Check code for lint errors
poetry run ruff check .

# Auto-fix fixable lint errors
poetry run ruff check --fix .

# Makefile shortcut
make ruff
```

---

## 5. Docker Smoke Check Script (`scripts/docker_smoke_check.py`)

To verify that a built Docker image or container starts properly and serves SFTP/WebDAV requests:

```bash
docker run --rm -v "$PWD/scripts:/app/scripts:ro" xyvran/tgdcfs:latest \
  python scripts/docker_smoke_check.py
```
