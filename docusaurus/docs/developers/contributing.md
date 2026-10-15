---
sidebar_position: 4
title: Contribution Guide & Workflows
description: Pull request workflow, commit guidelines, CHANGELOG management, and release process.
---

# 🤝 Contribution Guide & Workflows

We welcome contributions from the developer community! Follow this guide to set up your GitHub repository fork, configure repository secrets, understand automated CI workflows, and submit pull requests.

---

## Setting Up Your GitHub Repository Fork

To contribute code to TGDCFS, you should create a personal fork of the main repository.

### 1. Fork the Repository
1. Navigate to the main repository at [https://github.com/Xyvran/tgdcfs](https://github.com/Xyvran/tgdcfs).
2. Click the **Fork** button in the top-right corner to create a copy under your GitHub user account (e.g., `github.com/your-username/tgdcfs`).

### 2. Clone Your Fork Locally
Clone your fork to your workstation:
```bash
git clone https://github.com/your-username/tgdcfs.git
cd tgdcfs
```

### 3. Add the Upstream Remote
Configure the original repository as an `upstream` remote so you can sync changes from `master`:
```bash
git remote add upstream https://github.com/Xyvran/tgdcfs.git
git remote -v
```

To keep your local `master` branch up to date:
```bash
git checkout master
git fetch upstream
git merge upstream/master
git push origin master
```

---

## Configuring GitHub Repository Secrets (For Forks)

When running GitHub Actions workflows on your own fork or upstream repository, certain workflows utilize repository secrets:

| Secret Name | Required? | Purpose & Description |
| :--- | :---: | :--- |
| `DOCKERHUB_USERNAME` | Optional | Your Docker Hub account username. Used by `docker-preview.yml`, `docker-publish.yml`, and `release.yml` to log into Docker Hub. |
| `DOCKERHUB_TOKEN` | Optional | Personal access token / write token for Docker Hub. Required alongside `DOCKERHUB_USERNAME` to push built images to a Docker registry. |
| `CODECOV_TOKEN` | Optional | Token from [Codecov](https://codecov.io/) used by `test.yml` to upload test coverage reports. |

### How GitHub Actions Behaves on Forks Without Secrets:
- **No Secret Errors**: Workflows are designed to detect missing registry secrets gracefully.
- **`docker-preview.yml`**: In a fork or PR without `DOCKERHUB_TOKEN`, the workflow still checks out code, builds both Docker images (`tgdcfs` and `tgdcfs-fe`), runs the automated SFTP and Nginx smoke tests, and verifies data directory pinning (`TGDCFS_DATA_DIR`). It simply skips the `docker login` and image `push` steps.
- **`test.yml`**: Runs full `pytest` suite, `mypy` typechecks, and `ruff` linting. Codecov upload step is skipped or non-blocking if `CODECOV_TOKEN` is absent.

---

## Contribution Workflow Step-by-Step

### Step 1: Create a Feature Branch
Create a new branch off `master` with a descriptive name:
```bash
git checkout master
git pull upstream master
git checkout -b feature/your-feature-name
```

### Step 2: Implement Your Changes
- Write clean, well-typed Python 3.13 code.
- Ensure new features include unit tests in `tests/`.
- Ensure all docstrings and user-facing features are updated in documentation.

### Step 3: Record Changes in `CHANGELOG.md`
Every pull request that modifies behavior or adds features must update `CHANGELOG.md` under the `[Unreleased]` section:

```markdown
## [Unreleased]

### Added
- Added support for custom upload buffer paths in SFTP configuration.

### Fixed
- Fixed transient timeout issue during large multi-source downloads.
```

### Step 4: Run Tests & Quality Checks
Before committing, verify that all quality checks pass locally:
```bash
poetry run pytest
poetry run mypy .
poetry run ruff check .
```

### Step 5: Commit & Push
Commit your changes with concise, standard commit messages (50 chars max for subject line):
```bash
git add .
git commit -m "feat(sftp): add support for custom upload buffer path"
git push origin feature/your-feature-name
```

### Step 6: Open a Pull Request
Go to the [Xyvran/tgdcfs GitHub Repository](https://github.com/Xyvran/tgdcfs) and open a Pull Request targeting `master`.

---

## Deep Dive: GitHub Actions CI/CD Workflows

TGDCFS uses 5 distinct GitHub Actions workflows in `.github/workflows/`:

### 1. Test and Coverage (`.github/workflows/test.yml`)
- **Triggers**: On every `push` (all branches) and `pull_request` targeting `master`.
- **Steps**:
  1. Sets up Python 3.13 and Poetry.
  2. Caches Python virtual environments (`.venv`).
  3. Runs `mypy` static type analysis (`poetry run python -m mypy .`).
  4. Runs `ruff` code formatting and linting (`poetry run python -m ruff check .`).
  5. Runs complete unit test suite with coverage (`poetry run pytest --cov`).
  6. Uploads coverage report to Codecov.

### 2. Docker Preview Builds (`.github/workflows/docker-preview.yml`)
- **Triggers**: On `push` to any branch except `master`.
- **Matrix**: Builds both `tgdcfs` (backend) and `tgdcfs-fe` (frontend).
- **Steps**:
  1. Tags preview image with commit hash: `preview-<commit_hash>` and `preview`.
  2. Uses Docker Buildx with GitHub Actions cache.
  3. **Local Container Verification**: Runs container smoke tests:
     - For backend: Runs `python scripts/docker_smoke_check.py` and verifies `TGDCFS_DATA_DIR` path pinning with default and unprivileged UIDs (`--user 12345`).
     - For frontend: Starts Nginx container and curls `http://127.0.0.1:8080/tgdcfs/`.
  4. **Conditional Registry Push**: If `DOCKERHUB_TOKEN` secret is present, publishes preview tags to Docker Hub.

### 3. Docker Publish on Master (`.github/workflows/docker-publish.yml`)
- **Triggers**: Automatically runs via `workflow_run` when `Test and Coverage` workflow successfully completes on branch `master`.
- **Builds & Pushes**: Multi-architecture (`linux/amd64`, `linux/arm64`) images tagged `:latest` and `:<commit_hash>` for both `xyvran/tgdcfs` and `xyvran/tgdcfs-fe`.

### 4. Release Automation (`.github/workflows/release.yml`)
- **Triggers**: On push of git tag `vX.Y.Z` or when a commit on `master` bumps the `version` in `pyproject.toml`.
- **Steps**:
  1. Auto-creates git release tag `vX.Y.Z` if not already created.
  2. Parses release notes for `[X.Y.Z]` from `CHANGELOG.md` using `scripts/changelog.py`.
  3. Creates or updates GitHub Release entry.
  4. Attaches Docker release tags (`X.Y.Z` and `X.Y`) to the published images using `docker buildx imagetools`.

### 5. GitHub Pages Deployment (`.github/workflows/gh-pages.yml`)
- **Triggers**: On push to `master`.
- **Steps**: Builds Next.js web application (`tgdcfs-gh-pages/`) and deploys static export to GitHub Pages.

---

## Release Process (For Maintainers)

TGDCFS strictly follows [Semantic Versioning (SemVer)](https://semver.org/).

When cutting a new release on `master`:

1. Rename `[Unreleased]` in `CHANGELOG.md` to `[X.Y.Z] - YYYY-MM-DD` and add a new empty `[Unreleased]` section above it.
2. Bump `version` in `pyproject.toml` to `X.Y.Z`.
3. Commit and push to `master`.
4. Create and push tag `vX.Y.Z`:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
5. GitHub Actions `release.yml` will automatically publish the release notes and build multi-arch Docker tags (`xyvran/tgdcfs:X.Y.Z` and `xyvran/tgdcfs:X.Y`).
