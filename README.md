[![Tests](https://img.shields.io/github/actions/workflow/status/Xyvran/tgdcfs/test.yml?branch=master&style=for-the-badge&label=tests)](https://github.com/Xyvran/tgdcfs/actions/workflows/test.yml)
[![Docker build](https://img.shields.io/github/actions/workflow/status/Xyvran/tgdcfs/docker-publish.yml?branch=master&style=for-the-badge&label=docker%20build)](https://github.com/Xyvran/tgdcfs/actions/workflows/docker-publish.yml)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://hub.docker.com/r/xyvran/tgdcfs)

# tgdcfs

Telegram and Discord as storage backends behind one WebDAV and SFTP
server.

tgdcfs is a copy of [tgfs](https://github.com/Xyvran/tgfs) (taken at
commit `7464666`) that grows a second storage backend, Discord. Every
file system has a primary store and optional mirror stores, and a store
can be a Telegram channel or a Discord channel in either role. The
design is described in
[docs/design/architecture-plan.md](docs/design/architecture-plan.md);
the Discord backend, cross-backend mirroring in both directions and
background replication are in.

Many thanks to [WheatCarrier](https://github.com/TheodoreKrypton/tgfs)
for creating the original tgfs project this repository is built upon.

## Coming from tgfs

* The Python package, the Docker image (`xyvran/tgdcfs`) and the
  environment variables are renamed. `TGFS_DATA_DIR` and
  `TGFS_CONFIG_FILE` still work with a deprecation warning; the new
  names are `TGDCFS_DATA_DIR` and `TGDCFS_CONFIG_FILE`.
* The data directory inside the image is `/home/tgdcfs/.tgdcfs`
  (was `/home/tgfs/.tgfs`), so adjust the volume mount when switching
  images.
* A `config.yaml` written for tgfs loads unchanged: the top-level
  `tgfs:` block is read as `tgdcfs:` with a deprecation warning.
* The metadata format is unchanged; a channel written by tgfs is served
  by tgdcfs as is.

## Tested Clients
* [rclone](https://rclone.org/)
* [Cyberduck](https://cyberduck.io/)
* [WinSCP](https://winscp.net/)
* [Documents](https://readdle.com/documents) by Readdle
* [VidHub](https://okaapps.com/product/1659622164)

WinSCP, Cyberduck and rclone speak both protocols; the OpenSSH `sftp`
command line and `sshfs` work against the SFTP interface as well.

## Features
* Upload and download files to/from a private Telegram channel via WebDAV
* Uploads keep their modification time: clients that set it after the
  upload (CarotDAV, Windows Explorer, WebDAV sync tools) are honoured via
  PROPPATCH; the Windows attributes sent along with it are accepted and ignored
* **Optional SFTP interface** serving the same tree next to WebDAV (see below)
* Group files on Telegram channels into folders
* Infinite versioning of files and folders (Folder versioning is only available when Metadata is maintained on Github repository)
* Importing files that are already on Telegram (Only via the Telegram Mini App)
* File size is unlimited (larger files are chunked into parts but appear as a single file to the user)
* Live streaming of videos
* **Optional at-rest encryption** (AES-256-GCM, see below)
* **Optional channel redundancy** (RAID-1-style mirroring to extra channels, see below)
* **Optional local cache** on the data volume: uploads are staged once for
  every mirror, repeated reads are served from disk, and per file system
  uploads can be acknowledged as soon as they are on the local disk and
  distributed to every store in parallel (see below)
* **Optional multi-source downloads**: one download pulls pieces from
  the primary and every mirror at once (see below)


## SFTP interface

Next to the HTTP surface (WebDAV under `/webdav`, manager API under `/api`)
TGDCFS can serve the very same file tree over SFTP. It is off by default;
switch it on with an `sftp` block in `config.yaml`:

```yaml
tgdcfs:
  sftp:
    enabled: true
    host: 0.0.0.0
    port: 2222
    host_key_file: sftp_host_key
    # authorized_keys_dir: sftp_authorized_keys
    # upload_buffer_size_mb: 64
    # upload_buffer_dir: ''
```

Both interfaces run in the same process and go through the same core, so
what you see over SFTP is what you see over WebDAV — including at-rest
encryption, channel redundancy and the manager UI's task list. SSH cannot
share a socket with HTTP, hence the separate port.

**Layout and accounts.** The root lists one directory per configured
file system, exactly like `/webdav` does, and everything below it is that
file system's tree. Accounts are the `tgdcfs.users` from the same config: a user
with `readonly: true` can list and download but gets "permission denied" on
uploads, deletes, renames and directory creation. With no users configured
at all, SFTP allows anonymous read-only access — the same deal the HTTP
side offers.

```bash
sftp -P 2222 <user>@<host>          # interactive
sshfs -p 2222 <user>@<host>:/ /mnt  # mount it
rclone config create tgdcfs sftp host=<host> port=2222 user=<user> pass=<pass>
```

**Host key.** On the first start an ed25519 key is generated at
`host_key_file` (relative paths resolve against the TGDCFS data directory)
with mode 0600, and its fingerprint is logged. Back it up together with the
rest of that directory: without it every restart presents a new host key
and clients refuse to connect until their `known_hosts` entry is cleared.

**Public keys** are optional. Point `authorized_keys_dir` at a directory
holding one file per user — named after the username, in the usual
`authorized_keys` format — and those users may log in with a key. They
still have to be listed under `tgdcfs.users` so the readonly flag keeps
applying; everyone else falls back to password authentication.

**Uploads are streamed when possible.** SFTP does not announce the file
size up front. Telegram's MTProto API supports streamed uploads
(`upload.saveBigFilePart` with `file_total_parts = -1` until the final
part — see the official [files documentation](https://core.telegram.org/api/files)).
For the common sequential write path (OpenSSH `sftp`, rclone, WinSCP,
etc.) TGDCFS therefore feeds the uploader as bytes arrive; the total size
is only known when the client closes the handle. Non-sequential or sparse
writes still fall back to a temporary spool (memory up to
`upload_buffer_size_mb`, then disk under `upload_buffer_dir`) so correctness
is preserved. A transfer that is cut off mid-flight is discarded rather
than committed, so an interrupted upload never truncates an existing file.

**Known limits**, all of them shared with the WebDAV interface: files
cannot be appended to or partially updated (every write stores a new
version), moving between two channels is refused, and there are no symlinks
or real POSIX permissions — `chmod`, `chown` and `touch -t` are accepted
and ignored so clients like `rsync` do not abort. (Over WebDAV, a
modification time set with PROPPATCH is stored, see above.)

## Stores and file systems

A **store** is one channel of one backend (Telegram or Discord), under
a name of your choosing. A **file system** is what you see
as a top-level directory over WebDAV and SFTP: it has one *primary*
store and any number of *mirror* stores, and the same store can play
either role. Swapping primary and mirror is a config change.

The [config generator](https://xyvran.github.io/tgdcfs/config-generator/)
writes this file in the browser, with the same validation rules the
loader applies; nothing you type leaves the page. The
[getting-started guide](https://xyvran.github.io/tgdcfs/getting-started/)
walks through the bots and channels it asks for.

```yaml
backends:
  telegram:
    api_id: 12345
    api_hash: "..."
    bot:
      session_file: bot.session
      tokens: ["..."]
    delete_messages_on_remove: false

stores:
  tg-main:  {backend: telegram, channel: "-1001234567890"}
  tg-spare: {backend: telegram, channel: "-1009876543210"}

filesystems:
  media:
    primary: tg-main
    mirrors: [tg-spare]
    mode: auto           # auto (default) | forward | reupload
    sync: inline         # inline | background | tee; default: background when a mirror re-uploads
    strict: false        # true: an upload fails when mirroring fails
    metadata:
      type: pinned_message   # or github_repo, see below

tgdcfs:
  users: {...}
  jwt: {...}
  server: {host: 0.0.0.0, port: 1900}
```

The tgfs layout (`telegram.private_file_channel`, `tgfs.metadata` keyed
by channel id, `telegram.redundancy`) is still accepted and translated
on load, so a tgfs `config.yaml` works unchanged. The two layouts cannot
be mixed in one file.

Rules the loader enforces: every store a file system names exists; a
store is the primary of at most one file system; a file system does not
mirror into its own primary; a store that is the primary of one file
system is not a mirror of another unless that file system sets
`allow_shared_store: true` (two trees writing into one channel invites
id confusion); `strict: true` requires `sync: inline`.

The manager API describes the result: `GET /api/stores` lists the stores
with their backend, key and capabilities, `GET /api/filesystems` the
file systems with their primary, mirrors and mirroring settings.

## Discord backend

Discord channels can be stores too, as primaries or as mirrors. A
Discord store partitions files into attachments of
`max_file_size_bytes` (see the limits below), sends descriptors that
exceed a bot's 2000 characters as a small JSON attachment, and streams
downloads from the CDN with HTTP range requests.

Because a Discord part is small, a replica lists many of them: a 10 GB
file mirrored to Discord has over a thousand part ids in its descriptor.
A Telegram store therefore moves a descriptor longer than Telegram's 4096
characters into a small `overflow.json` document as well; the text
message keeps its id (the metadata refers to it) and points at the
document, and reads resolve the pointer. A superseded document is deleted
when the descriptor is edited again.

Several bot tokens share the work round-robin. A Discord bot may edit
its own messages only, so an edit of a descriptor or a replaced
attachment is always made by the bot that sent the message: the store
remembers who sent what and looks the sender of an older message up
once. A message whose sender was removed from `bot_tokens` cannot be
edited by anyone and is replaced by a fresh one on its next change.

```yaml
backends:
  discord:
    bot_tokens: ["..."]            # one or more bots, used round-robin
    max_file_size_bytes: 10000000  # per attachment; raise to match the server
    delete_messages_on_remove: false
    # upload_max_retries: 10
    # upload_retry_interval: 5
    # max_concurrent_uploads: 3     # parts in flight; default: one per bot, at least 3
    # max_concurrent_downloads: 3

stores:
  dc-main: {backend: discord, channel: "123456789012345678"}

filesystems:
  notes:
    primary: dc-main
    mirrors: [tg-spare]            # a Telegram mirror of a Discord primary works
    metadata: {type: github_repo, github_repo: {...}}
```

Setting up the bot: create an application at
https://discord.com/developers/applications, add a bot, enable the
*Message Content* intent, and invite it to your server with *View
Channel*, *Send Messages*, *Manage Messages*, *Read Message History*,
*Attach Files* and *Pin Messages* on the channels you use as stores.
The channel id is the numeric id from *Copy Channel ID* (developer
mode).

Limits that shape a Discord store (as of September 2026):

* A bot may attach 20 MB per file on an unboosted server (10 MB before
  August 2026), 50 MB at boost level 2 and 100 MB at level 3. The
  default `max_file_size_bytes` is a conservative 10 MB; a value above
  the server's limit makes every upload fail.
* Roughly one message per second per channel is the practical send
  rate, so about 30 GB per hour per channel at 10 MB parts. Discord is
  a fine mirror for the valuable part of a library and a slow home for
  bulk media.
* Attachment URLs are signed and expire after about a day; TGDCFS
  fetches the message before every download, so this is invisible.
* Messages older than 14 days cannot be bulk deleted; they are deleted
  one by one, which is slow for large removals.
* There is no server-side copy: mirroring *into* a Discord store always
  re-uploads the bytes, and a Discord primary cannot be mirrored into
  Telegram by forwarding either.
* Mirroring a Telegram primary *into* Discord re-partitions every
  version into Discord-sized messages (a replica, see Mirroring) and
  re-uploads the bytes. Such a file system defaults to
  `sync: background` so uploads do not wait for it.
* `pinned_message` metadata must fit one attachment; use
  `github_repo` metadata for anything but small trees on a Discord
  primary.

Storing large volumes of data through a bot is not what Discord is
for, and a bot or server can be terminated. Treat a Discord primary as
something that is mirrored elsewhere.
