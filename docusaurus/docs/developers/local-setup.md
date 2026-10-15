---
sidebar_position: 2
title: Local Repository & Environment Setup
description: Step-by-step guide to cloning the repo and configuring your local development environment.
---

# 🛠️ Local Repository & Environment Setup

This step-by-step guide will help you set up the TGDCFS codebase on your local machine for active development.

---

## Prerequisites

Before starting, ensure your system has the following installed:

- **Git**: Version 2.30+
- **Python**: Version **3.13** or higher (`python3 --version`)
- **Poetry**: Version **2.0** or higher (`poetry --version`)
- **Node.js**: Version **20.0+** (only required if working on the Next.js frontend or Docusaurus site)

---

## Step 1: Clone the GitHub Repository

Clone the TGDCFS repository from GitHub to your local workstation:

```bash
git clone https://github.com/Xyvran/tgdcfs.git
cd tgdcfs
```

---

## Step 2: Install Python Dependencies with Poetry

TGDCFS uses **Poetry** for dependency management and virtual environments.

1. Tell Poetry to use your Python 3.13 interpreter:
```bash
poetry env use python3.13
```

2. Install all core dependencies and development tools (`mypy`, `ruff`, `pytest`, `httpx`):
```bash
poetry install
```

3. Verify that the virtual environment was created successfully:
```bash
poetry run python --version
```
*(Should output `Python 3.13.x`)*

---

## Step 3: Install Git Pre-Commit Hooks

TGDCFS uses `pre-commit` to enforce code quality, linting, and formatting checks automatically before every commit.

Install the pre-commit git hooks:
```bash
poetry run pre-commit install
```

Now, every time you execute `git commit`, `pre-commit` will automatically run formatting and linting checks against your staged files!

---

## Step 4: Run TGDCFS Locally

To run the main application server locally in development mode:

1. Create a minimal development config `config.yaml` or copy `demo-config.yaml`:
```bash
cp demo-config.yaml config.yaml
```

2. Start the main application:
```bash
poetry run python main.py
```

The server will initialize and start listening for WebDAV and Manager API requests on `http://localhost:1900`.

---

## Step 5: Building & Testing Docker Containers Locally

TGDCFS provides multi-stage `Dockerfile` (for the Python backend service) and `Dockerfile.frontend` (for the Nginx + Next.js web frontend).

### Prerequisites for Docker Development
- **Docker Engine** (20.10+) or **Docker Desktop** / **Podman**
- **Docker Buildx** (recommended for multi-architecture builds)

---

### Building the Backend Container (`Dockerfile`)

Build the backend Docker image using the main `Dockerfile`:

```bash
docker build -t tgdcfs:local -f Dockerfile .
```

#### Key Architecture Notes:
- **Multi-Stage Build**: Stage 1 (`builder`) installs Poetry and builds Python packages into `/usr/local`. Stage 2 installs runtime system dependencies and copies wheels over, resulting in a minimal footprint.
- **Non-Root User**: The image runs as non-root user `tgdcfs` (UID/GID created in container).
- **Data Directory Pinning (`TGDCFS_DATA_DIR`)**: Explicitly pinned to `ENV TGDCFS_DATA_DIR=/home/tgdcfs/.tgdcfs`. This guarantees that even if run with custom user IDs (`docker run --user 12345`), path expansion resolves consistently to `/home/tgdcfs/.tgdcfs`.

---

### Running the Backend Container

To run your locally built TGDCFS container with your local configuration and persistent data directory:

```bash
docker run -d \
  --name tgdcfs-local \
  -p 1900:1900 \
  -p 2222:2222 \
  -v "$PWD/config.yaml:/home/tgdcfs/.tgdcfs/config.yaml:ro" \
  -v "$PWD/data:/home/tgdcfs/.tgdcfs" \
  tgdcfs:local
```

#### Container Ports Summary:
- **`1900`**: WebDAV server & Manager REST API endpoint.
- **`2222`**: AsyncSSH SFTP server (active when `tgdcfs.sftp.enabled: true` in config).

---

### Running Automated Docker Container Smoke Checks

TGDCFS includes an automated smoke test script (`scripts/docker_smoke_check.py`) that tests container boot, runtime dependencies, host key generation, and SFTP authentication without requiring live Telegram API credentials.

Run the smoke test against your built image:

```bash
docker run --rm \
  -v "$PWD/scripts:/app/scripts:ro" \
  tgdcfs:local \
  python scripts/docker_smoke_check.py
```

#### What the Smoke Test Verifies:
1. **Runtime Imports**: Ensures all binary C extensions (`asyncssh`, `cryptography`, `argon2-cffi`, `uvloop`) import without missing dynamic libraries.
2. **SFTP Server Initialization**: Starts AsyncSSH with a temporary in-memory configuration and generates host key files in `TGDCFS_DATA_DIR`.
3. **Authentication Verification**: Connects via AsyncSSH to test valid credential acceptance and invalid credential rejection.
4. **Custom UID / Non-Root Safety**: Verifies `TGDCFS_DATA_DIR` path resolution under arbitrary UIDs (`docker run --user 12345`).

---

### Building & Testing the Web Frontend Container (`Dockerfile.frontend`)

The web frontend (`tgdcfs-gh-pages/`) is built using Next.js static exports and served by Nginx.

#### Build the Frontend Image:
```bash
docker build -t tgdcfs-fe:local -f Dockerfile.frontend .
```

#### Run the Frontend Container:
```bash
docker run -d \
  --name tgdcfs-fe-local \
  -p 8080:80 \
  tgdcfs-fe:local
```

Access the frontend mini-app at `http://localhost:8080/tgdcfs/`.

#### Smoke-Testing Frontend Image via CLI:
```bash
curl -f -I http://localhost:8080/tgdcfs/
```
*(A HTTP `200 OK` confirms Nginx is correctly serving the exported Next.js app on path `/tgdcfs/`).*
