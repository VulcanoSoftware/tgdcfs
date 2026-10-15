---
sidebar_position: 4
title: Contribution Guide & Workflows
description: Pull request workflow, commit guidelines, CHANGELOG management, and release process.
---

# 🤝 Contribution Guide & Workflows

We welcome contributions from the developer community! Follow this guide to ensure a smooth pull request review and merge process.

---

## Contribution Workflow Step-by-Step

### Step 1: Create a Feature Branch
Create a new branch off `master` with a descriptive name:
```bash
git checkout master
git pull origin master
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

## CI Automated Workflows & Preview Docker Builds

When you open or update a Pull Request:

1. **Test Workflow (`test.yml`)**: Runs `pytest`, `mypy`, and `ruff` across Python 3.13 environments.
2. **Docker Preview Workflow (`docker-preview.yml`)**: Automatically builds a preview Docker image tagged:
   ```bash
   docker pull xyvran/tgdcfs:preview-<commit_hash>
   docker pull xyvran/tgdcfs-fe:preview-<commit_hash>
   ```
   This allows you and reviewers to test your exact branch on a live machine before merging!

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
