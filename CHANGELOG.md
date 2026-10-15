# Changelog

All notable changes to tgdcfs are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0.0, a minor release may change behaviour or
configuration defaults; such changes are listed under **Changed** and marked
as breaking where an existing setup needs attention.

Every release is a Git tag `vX.Y.Z` with a matching
[GitHub release](https://github.com/Xyvran/tgdcfs/releases), and the Docker
images `xyvran/tgdcfs` and `xyvran/tgdcfs-fe` carry the tags `X.Y.Z` and `X.Y`
next to the commit tag. Releases up to and including 0.6.1 were tagged after
the fact on the commits that shipped them.

## [Unreleased]

### Added

- This changelog, SemVer release tags and GitHub releases. A pushed tag
  `vX.Y.Z` publishes the release notes from this file and adds the version
  tags to the Docker images of that commit.

### Changed

- Cache evictions are logged as one line per run, `Cache: evicted N entries
  (size) to make room` or `… for the disk headroom`, instead of one line per
  entry; the per-entry lines are at debug level.

### Fixed

- A large upload into a cache full of small entries no longer freezes the
  server. Making room for it rewrote the cache index after every evicted
  entry on the event loop; with about 10,000 cached files that held every
  request for several minutes, so WebDAV clients timed out ("Connection timed
  out after 20 seconds of inactivity"), directory listings failed and the
  Telegram and Discord connections dropped. Evictions now run in one pass and
  write the index once; the background sweep and bulk removals do the same.
- Large files mirrored to Discord can be written with a Telegram primary.
  The file descriptor lists every Discord part (10 MB each), so a 10 GB file
  needs over a thousand ids and no longer fit Telegram's 4096-character
  message: the upload finished in both stores, then failed with
  `MessageTooLongError`, the client got a 500 and the file stayed at 0 bytes.
  Such descriptors now go into a small `overflow.json` document that the
  descriptor message points at; the message keeps its id. Older versions
  cannot read these descriptors.
- Files whose descriptor went into an `overflow.json` document no longer
  show up with 0 bytes after a restart. The document was read "to the end"
  (`-1`), which only the block cache understood; with `chunk_cache_mb: 0`
  (the default) the download was refused as an invalid range, the
  descriptor counted as lost and the file as empty. Nothing was lost: the
  files read correctly again with this fix.
- With `sync: tee` an upload is no longer written to the local cache as
  well, as the README already promised: the mirrors take the bytes from the
  upload stream, so the staged copy was never read and only cost disk space
  and evictions. `stage_uploads` now only applies to `sync: inline` and
  `sync: background`.
- The `xyvran/tgdcfs-fe` image is published again. Its static build ran
  under arm64 emulation and hit the six-hour job limit; it now runs once on
  the build host, and only the nginx stage is built per platform.

### Security

- With `encrypt_names: true` and `github_repo` metadata, the commit messages
  in the metadata repository no longer contain plaintext file and directory
  names ("Create file reference for movie.mp4"); they name the encrypted
  segment that the path already shows. Commits made by earlier versions keep
  their plaintext messages until the repository history is rewritten or the
  repository is replaced.

## [0.6.1] - 2026-10-04

### Changed

- When the local cache refuses an entry, the log now says why: which limit is
  hit (`max_size_mb`, `max_files` or the `min_free_mb` disk headroom) with the
  figures, and what holds the rest (pinned entries, entries being written, or
  a disk filled by something outside the cache). The messages read
  `Cache: not caching …`, `… is only partly cached: …` and
  `… is read without the cache: …`.

### Fixed

- A client that hangs up while its request body is being read is no longer
  logged as "Exception in ASGI application" with a traceback; it is logged at
  debug level only.

## [0.6.0] - 2026-09-28

### Added

- `sync: tee` for file systems with mirrors that re-upload (Discord on either
  side, another backend, or `mode: reupload`). The client's upload stream is
  split: the primary consumes it as before and each such mirror uploads the
  same bytes at the same time, without reading them back from the primary and
  without touching the disk. A write lasts as long as the slowest store. A
  mirror that fails mid-stream is detached and backfilled later; a failing
  primary aborts the mirrors. Mirrors that copy server-side are still
  forwarded afterwards.
- `tgdcfs.transfer.tee_buffer_mb` (default `64`): buffer per mirror in tee
  mode.
- Config generator: "Tee" in the Sync select, a "Tee Buffer per Mirror (MB)"
  field, and an on/off switch for the Telegram section like the Discord one,
  so a Discord-only config can be built from the form.

### Changed

- `sync: tee` is refused together with `strict: true` or `write_ack: cache`.
- Discord uploads read a part only once an upload slot is free, so a source
  faster than Discord is held back instead of being buffered whole in memory.
- `backends.discord.max_concurrent_uploads` now defaults to one part per bot,
  at least 3, so every configured bot stays busy.

## [0.5.1] - 2026-09-27

### Fixed

- Discord stores with several bot tokens: editing a message (the mirrored
  file descriptor) went to the next bot of the round-robin and failed with
  403 / error 50005 almost every time, leaving stale descriptor copies in
  Discord mirrors. Edits now go to the bot that sent the message; a message
  whose sender is no longer configured is replaced by a fresh one.
- Listing a folder with thousands of files blocked the event loop for 10 to
  27 seconds per PROPFIND (and made Discord bots miss their heartbeats). File
  lookups by name in a folder are now constant-time.

## [0.5.0] - 2026-09-26

### Added

- Config generator: "Load Existing config.yaml" reads a config back into the
  form, both the current layout and the tgfs layout, and lists keys the form
  has no field for instead of dropping them silently.
- Config generator: a Docker panel with the `docker run` command and a
  downloadable `docker-compose.yml` for the config at hand (ports, data
  volume, passphrase variable from a `.env` file).
- Config generator: a read-only checkbox for users.

### Changed

- Read cache: large files are now cached block by block as they are read.
  Previously a version had to pass `max_file_size_mb`, fit the whole budget
  and leave the disk headroom before a single block was cached, so large
  files were never cached at all. `max_file_size_mb` now limits only what is
  staged whole (uploads and mirror downloads).

## [0.4.1] - 2026-09-26

### Fixed

- A PROPFIND on encrypted files left one empty read-cache entry per listed
  file. They cost no budget, but piled up without a `max_files` limit, could
  evict real content, and their sparse files made a plain `tar` of the data
  directory as large as every file listed.
- `MKCOL` and `PUT` directly below the WebDAV root (outside every configured
  file system) failed with 500. They now answer 403, and a `PUT` whose parent
  folder does not exist answers 409, as RFC 4918 requires. Upload tools such
  as rclone and tgsaver now get a proper answer.

## [0.4.0] - 2026-09-26

### Added

- Local cache on the data volume (`tgdcfs.cache`, off by default). It holds
  ciphertext only and never fails a transfer: a disk error or a full cache
  means the transfer runs without it.
  - Staged uploads (`stage_uploads`): mirrors that re-upload read the bytes
    from the cache instead of downloading them from the primary again.
  - Read cache (`keep_for_reads`): repeated reads are served from disk, filled
    block by block (`block_kb`).
  - Budget and limits: `max_size_mb` (default 20480) is a hard ceiling,
    `max_files`, `max_file_size_mb`, and `min_free_mb` (default 1024) keeps
    free disk space for everything else in the data directory.
  - A background sweep every 15 minutes and at start removes orphans, releases
    stale pins, drops entries unread for `max_age_hours` (default 0, off) and
    evicts down to `target_fill_percent` (default 90) of the budget.
- Write-back uploads per file system (`write_ack: cache`, default `primary`):
  a PUT is answered once the bytes are in the local cache, and a worker
  distributes them to the primary and every mirror in parallel. Requires
  `cache.enabled`, refused with `strict: true`. Until the primary has the
  file, it exists only on the local disk.
- Multi-source downloads per file system (`read_parallel`, default off;
  `read_sources` to limit the stores): a download is split into pieces served
  by every store that holds a complete copy, with per-piece failover.
- `tgdcfs.transfer.upload_parts_in_flight` (default 1): upload several
  Telegram parts at once from a seekable source such as the cache.
- `tgdcfs.transfer.read_parallel_window`: reorder window for multi-source
  reads.
- Manager API: `GET /api/cache` (budget, entries, hits and misses, free disk
  space, last sweep) and `POST /api/cache/evict`. The mini app shows the cache
  in the "Mirrors and replication" dialog.
- Config generator: a "Local Cache" section and the per-file-system fields
  `write_ack`, `read_parallel` and `read_sources`. Only values that differ
  from the defaults are written.

## [0.3.0] - 2026-09-26

### Added

- WebDAV `PROPPATCH`: `getlastmodified` and `Win32LastModifiedTime` set the
  modification time of the latest file version. The Windows attributes
  CarotDAV sends along (`Win32CreationTime`, `Win32LastAccessTime`,
  `Win32FileAttributes`) are accepted and ignored, so CarotDAV no longer
  reports a failed upload. Other properties are refused with 403.
- A startup warning when an explicit `sync: inline` or `strict: true` makes
  every write wait for a mirror that re-uploads.

### Changed

- **Breaking for configs without `sync`:** a file system that leaves out
  `sync` now gets `background` when a mirror re-uploads (another backend,
  Discord on either side, `mode: reupload`) and `inline` for Telegram to
  Telegram forwarding. Before, it was always `inline`, which made large
  uploads with a Discord mirror time out.
- The config generator writes `sync` only when it differs from that derived
  default, and `strict` only when true.
- GitHub metadata loads with a fixed number of API calls regardless of the
  folder count (one recursive tree call, folder dates cached on disk as
  `dir-timestamps-<repo>-<branch>.json`). Startup went from minutes to
  seconds on large trees.

### Fixed

- The Discord login is retried on server errors (5xx, refused connections)
  instead of taking the server down at startup; a rejected token still fails
  at once.
- A request for a path outside every file system (e.g. a browser's
  `/webdav/favicon.ico`) answers 404 instead of raising a `KeyError`.
- WebDAV reported the boot time as the modification date of every folder that
  contains files; the folder dates from the repository history are kept now.

## [0.2.0] - 2026-09-25

### Added

- Discord as a storage backend (`backends.discord`): a Discord channel can be
  a primary or a mirror store. Files are cut into parts of
  `max_file_size_bytes` (default 10 MB), several bot tokens are used in
  parallel, long descriptors travel as attachments, and transient failures
  are retried. Adapted from [dcfs](https://github.com/VulcanoSoftware/dcfs).
- Store abstraction and a new config layout: `backends`, named `stores` and
  `filesystems` with a `primary` and optional `mirrors`. Swapping primary and
  mirror is a config change. The tgfs layout (`private_file_channel`,
  `metadata` per channel, `redundancy`) is still read and translated on load.
- Cross-backend mirroring with replicas in the mirror's own part layout, and
  reads that fail over part by part across copies and replicas
  (`read_preference` sets the order).
- Background replication (`sync: background`) with a persistent queue
  (`replication_queue.json` in the data directory) and retries.
- Manager API: `GET /stores`, `GET /filesystems`, `GET /replication/queue`,
  `POST /replication/retry`.
- Frontend: the config generator edits stores and file systems and has an
  optional Discord section; the getting-started guide covers the Discord bot;
  the mini app has a "Mirrors and replication" dialog.

## [0.1.0] - 2026-09-24

### Added

- tgdcfs, bootstrapped from [tgfs](https://github.com/Xyvran/tgfs) at commit
  `7464666`: Telegram as storage with WebDAV and SFTP access, at-rest
  encryption and GitHub or Telegram metadata.
- Docker images `xyvran/tgdcfs` and `xyvran/tgdcfs-fe`; container user and
  data directory `/home/tgdcfs/.tgdcfs`.

### Changed

- Renamed from tgfs: package `tgdcfs`, environment variables `TGDCFS_*` and
  data directory `~/.tgdcfs`. The old `TGFS_*` variables and a top-level
  `tgfs:` config block still work with a deprecation warning.
- Files and metadata written by tgfs are read unchanged; the on-disk
  encryption format keeps its tgfs constants.

[Unreleased]: https://github.com/Xyvran/tgdcfs/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/Xyvran/tgdcfs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/Xyvran/tgdcfs/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/Xyvran/tgdcfs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/Xyvran/tgdcfs/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/Xyvran/tgdcfs/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/Xyvran/tgdcfs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Xyvran/tgdcfs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Xyvran/tgdcfs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Xyvran/tgdcfs/releases/tag/v0.1.0
