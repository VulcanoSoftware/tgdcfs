# SFTP interface

## Why

TGDCFS published its store over exactly one surface: a single uvicorn process
with WebDAV mounted at `/webdav` and the manager REST API at `/api`. That
covers WebDAV-capable clients and nothing else, while SFTP is what a great
many file managers, backup tools and mount helpers reach for first.

The goal was a second access interface of equal standing — same tree, same
accounts, same guarantees — not a reduced side door.

## Where it sits

```
        SFTP client                       WebDAV client
             |                                  |
      asyncssh listener                 uvicorn / FastAPI
   (tgdcfs/app/sftp/server.py)          (tgdcfs/app/__init__.py)
             |                                  |
             +--------------+   +---------------+
                            |   |
                       tgdcfs/core/ops.py  (Ops)
                            |
              FileApi / DirectoryApi / MetaDataApi
                            |
            repositories (+ encryption, + mirroring)
                            |
                        Telegram
```

Both front-ends are thin: they translate a protocol into `Ops` calls. That
is the whole reason the two behave identically — at-rest encryption, mirror
failover, upload task tracking and metadata batching all live below the
fork, so neither interface can drift from the other.

The SFTP layer deliberately does **not** build on the `asgidav` classes.
Those model WebDAV specifically — `copy_to`/`move_to` take destinations
still carrying the `/webdav/<client>` prefix — so reusing them would have
meant either bending SFTP paths into WebDAV shapes or reworking the WebDAV
layer. Going straight to `Ops` costs a little duplication in exchange for
leaving the running interface untouched.

## Layout and accounts

The namespace matches WebDAV exactly: the root is synthetic and lists one
entry per configured client, and `/<client>/<path>` addresses that client's
tree. `split_global_path()` in `tgdcfs/app/utils.py` is shared between the two.

Authentication reuses `tgdcfs.auth.basic.authenticate`, so the same
`tgdcfs.users` block drives both interfaces, including the anonymous
read-only fallback when no users are configured. Write enforcement mirrors
`READONLY_METHODS`: everything that mutates goes through `_require_write()`
and raises `SFTPPermissionDenied` for a readonly user.

Public key authentication is the one thing SFTP adds. It is off unless
`authorized_keys_dir` is configured, and even then the account must exist
in `tgdcfs.users` — the config stays the single source of truth for who may
write.

## The two hard parts

Everything in SFTP maps onto `Ops` cleanly except file I/O, where the
protocol and Telegram want opposite things.

### Reads: random access over a one-shot stream

SFTP reads are `read(handle, offset, size)` at arbitrary offsets, and
clients pipeline several at once. `Ops.download(path, begin, end, name)`
gives an async iterator that only moves forward.

`ReadHandle` keeps one open stream plus the offset it has reached:

* a read continuing where the last one stopped — the overwhelming majority —
  is served straight from the buffer and the stream;
* a backward seek, or a forward jump larger than `SEEK_FORWARD_THRESHOLD`
  (1 MiB), re-opens the stream at the new offset, which is exactly what a
  WebDAV range request does;
* a smaller forward gap is bridged by reading through, because re-opening
  costs a Telegram round trip while the skipped bytes are already in flight.

An `asyncio.Lock` serialises the handle so pipelined reads cannot interleave
into the shared buffer.

One asyncssh-specific wrinkle: before copying, a client may probe the source
handle with `SEEK_DATA`/`SEEK_HOLE` to skip sparse regions. Nothing stored
here is sparse, so `Handle.seek` answers with a single solid data region and
raises `ENXIO` past the end. Without it, transfers from an asyncssh-based
client fail outright — which is exactly what the integration test caught.

### Writes: a size that is not known in advance

Telegram uploads need the total size up front (it decides the part layout);
SFTP never sends it. So `WriteHandle` spools the payload — in memory up to
`upload_buffer_size_mb`, on disk beyond that, via `SpooledTemporaryFile` —
and only on `close()` streams it into `Ops.upload_from_stream` with the
size now known.

Buffering is not just a workaround for the missing size: it also makes
out-of-order and sparse writes work, since the spool is seekable, and it
keeps a dropped connection from committing a truncated file — `exit()`
aborts anything still open rather than closing it.

`Ops.touch()` is called at open time so the name shows up in a listing
while the upload is still running, matching the WebDAV `PUT` path.

#### Why it feels like two uploads, and write-behind

The spool makes an upload serial where WebDAV's is pipelined. WebDAV knows
the size from `Content-Length`, so `Ops.upload_from_stream` runs while the
body is still arriving. SFTP only learns the size at CLOSE, so the stores
get the first byte after the client has sent the last one, and the client
waits for the answer to that CLOSE the whole time. The wait grows with the
file, and a client with an inactivity timeout gives up and re-uploads.

Clients do not announce the size either (OpenSSH strips it from the
attributes of OPEN on purpose). There are two ways out, below: upload while
receiving with Telegram's streamed upload (`sftp.stream_uploads`), or change
who waits: `sftp.write_behind` answers the CLOSE once the spool is complete.

`PendingUploads` (`pending.py`) takes the finished spool and uploads it in a
background task. Until that is done it is the registry the SFTP layer asks
first:

* `stat`/listing report the spooled size, and `open` for reading serves
  from the spool (`SpoolReadHandle`), otherwise the placeholder created at
  open time -- empty -- would be what a client verifying its upload sees;
* remove and rename wait for pending uploads of the path (below it, for a
  folder), otherwise the upload would land after the removal and bring the
  file back, or write to the old name after the move;
* two uploads of one path run in the order they were closed;
* the spool is reference counted between the upload and any read handle, and
  closed when the last of them lets go;
* the data held back is capped (`write_behind_max_mb`): `submit` waits for
  room, which delays the CLOSE and so pushes back on a client that is faster
  than Telegram. A file larger than the cap is let through when nothing else
  is pending, so the cap cannot deadlock;
* `main()` drains the registry after the listener is closed.

#### Streaming without a known size

`write_behind` only moves the wait. The wait can be removed because Telegram
does allow an upload of unknown length. https://core.telegram.org/api/files
("Streamed uploads") describes it: `upload.saveBigFilePart` is used for every
part (even if the stream ends up under 10 MB), `file_total_parts` is `-1` on
every part but the last, the last part carries `ceil(total / part_size)`, and
`inputFileBig` is finished with that count. The size of the file is only
needed for the normal flow, where the total is announced with every part; it
never was a limit of the API, only of `FileUploader`, which used to compute
`total_parts` from the size first.

`sftp.stream_uploads` uses it:

* `WriteHandle` tracks the prefix written without holes (`_hwm`). Once a few
  MB have arrived (`STREAM_START_BYTES`) it starts `Ops.upload_from_unsized_stream`
  over a stream that grows with the spool (`_growing`) and keeps feeding it as
  the client writes. Bytes handed to the stream are spoken for: a later write
  below `_yielded` is refused, anything beyond may still be rewritten, and a
  hole at the end reads as zeros.
* `FileMessageFromStream(size=None)` is an *unsized* message: `get_size`
  raises, `read` is short only at the end of the stream, and the task shows no
  total.
* `FileUploader` streams it (`_upload_stream`): parts of one part size go out
  with `-1`, up to the worker count in parallel, while the stream is read at the
  pace of the upload. One part is read ahead, so the last part is known to be
  the last and no empty part is needed; it is sent only after every earlier
  part is confirmed, with the real count. A document holds at most `max_bytes`
  (2 GB); the rest of the stream is left unread for the next document.
* `TelegramStore._upload_unsized` loops over documents, numbering them from
  `part_offset`, and sends each as soon as it is complete. On any failure the
  messages already sent are deleted again, forced, because they never belonged
  to a file.
* `IStore.streams_unsized_uploads` says whether a store takes such a stream.
  Telegram does; one that does not (Discord needs every attachment's size
  first) gets segments of `sftp.stream_segment_mb`:
  `StoreFileContentRepository._upload_segments` fills a spool per segment, uploads
  it as a normal part of known size (`FileMessageFromSpool`) and fills the next
  while that is on its way.
* Encryption wraps the unsized message and encrypts until the plaintext ends;
  the ciphertext is what gets cut into documents or segments. Staging in the
  cache and tee-ing to mirrors need the size and are skipped; the mirrors copy
  the finished parts afterwards.

Trade-offs: bytes already sent on cannot be rewritten, the document appears in
the channel only when it is complete, and a stream stays on the big-file calls
and so skips the cheaper small-file path (hence the start threshold). This has
been tested against the API as documented and against fakes, not against the
real Telegram servers.

The cost of `write_behind` is the point where the client is told "done": the data is only in a
spool, in memory or a temp file, not in a store. A crash loses it; a failure
is logged and marked failed in the task list, not returned to the client. It
is therefore opt-in, and `write_ack: cache`, which keeps the bytes on the
data volume and survives a restart, stays the durable alternative. Only SFTP
sessions of this process see a pending file; WebDAV sees it once it is
stored.

## Lifecycle

`start_sftp_server()` returns `None` when disabled and otherwise an
`SSHAcceptor` that keeps serving in the background of the same event loop
as uvicorn. `main()` shuts it down in a `finally` around `run_server`. A
failure to start it (port taken, unwritable host key) is logged and leaves
the HTTP interface running — the second interface must never be able to
take down the first.

The host key is generated on first start (ed25519, mode 0600) and lives in
the TGDCFS data directory, because a key that changes on every restart makes
clients refuse to connect.

## Known limits

All of these are shared with WebDAV and documented in the README:

* uploads need scratch space the size of the file, and take the transfer
  time twice unless `write_behind` is on (which trades durability, above);
* no append and no partial update — every write stores a new version;
* moving between two clients is refused;
* no symlinks and no real permissions; `setstat` accepts and ignores, since
  refusing would abort otherwise fine `rsync` and `sftp put` transfers.

## Tests

`tests/tgdcfs/app/sftp/` covers the pieces in isolation (path translation,
attribute mapping, both handles, the server methods against a mocked
`Client`, and authentication) plus `test_integration.py`, which drives a
real asyncssh client against a listener on an ephemeral port. The
integration layer is worth its weight: it exercises asyncssh's own protocol
handlers and the OPEN/READ/WRITE/CLOSE handle bookkeeping that the unit
tests bypass.
