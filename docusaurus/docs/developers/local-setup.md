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
