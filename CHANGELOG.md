# Change Log

All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## [1.0.7-mp.4] - 2026-07-26

MailPortal patched build (fork `kempu/vandelay`). Carries the mp.1–mp.3 patches below plus
structured per-item export failures. The binary still reports version `1.0.7`; the patched
build is distinguished by its tag/artifact (`-mp.4`) and checksum.

### Added
- export: record every per-item failure as a structured row in the archive (Patch 5). Export
  reported failures only as the aggregate `failed=N` on its stdout summary line plus free-text
  warnings on stderr, so a control plane driving the run could tell that N objects were refused
  but never WHICH ones — the detail existed as prose and was parsed into nothing. A new
  `export_failures` table now carries object type, archive-local id, creation id (`e23`),
  message-id, size, full blake3 of the object's bytes, the blobId the target returned, the target
  error type and full detail, a blob-retrievability verdict and a timestamp. Every reconciler
  writes to it, not just the Email one: mailboxes, address books, calendars, contact cards,
  calendar events, file nodes and Sieve scripts all funnel their refusals through the same writer,
  so `inspect --failures` reporting nothing means nothing was refused rather than "nothing that
  happened to be mail". (The `failed` counters still also cover two things that are not per-item
  and therefore carry no row: a type whose whole pass aborted, and a `--prune` destroy the target
  rejected — neither names an archive object a row could be keyed on.) The primary key is
  `(type_name, local_id)`, so a re-run replaces the row instead of appending and the table never
  becomes an unbounded log; a row is deleted once the item reaches the target, so what remains is
  exactly the unresolved set. Writes are gated on `--dry-run` — this is the first time export
  touches the archive at all — and a write that fails is reported, never dropped: it is named on
  stderr and ends the run with exit 5 through its own channel rather than being charged to
  `failed=N`, because a row that could not be written is not an object the target refused and
  counting it there would put one unit in the very number the rows exist to explain that matches
  no item. The archive READS that fill a row in are held to the same rule: `size` and the blake3
  are queried out of the archive, and a query that fails is logged and named in that row's
  `error_detail` instead of collapsing into a null column — a null hash otherwise reads as "the
  bytes are gone", which is both untrue after a transient `SQLITE_BUSY` and a silent loss of the
  one field that correlates the item with the marker the target is still holding
  (`db/export_failures.rs`, `db/schema.sql`, `sync/export.rs`, `sync/export/email.rs`,
  `sync/export/sieve.rs`, `sync/export/tree.rs`, `sync/export/flat.rs`, `sync/export/uidtype.rs`,
  `inspect/failures.rs`, `sync/mod.rs`, `main.rs`).
- export: tell an orphaned de-duplication marker apart from a broken blob store (Patch 6). On a
  content-addressed target an upload is de-duplicated by content hash: the server answers 200 with
  a blobId without rewriting the bytes, because its metadata store already holds a marker for that
  hash. If the bytes behind that marker are lost, the marker keeps shadowing them — the upload
  keeps succeeding, `Email/import` keeps answering `blobNotFound`, and the stock self-heal
  (re-upload and retry, added upstream in 1.0.6 for #13) re-sends IDENTICAL bytes, which hash the
  same and are de-duped away again, so the condition can never self-heal. When a `blobNotFound`
  survives the re-upload, the returned blobId is now probed with `Blob/get` (new
  `blobxfer::lookup_blob`, `properties: ["size"]` so the server reads its own blob store without
  sending the message back over the wire, on a path that has already paid for two uploads). Upload
  accepted plus lookup not-found is recorded as `orphaned_marker`; a servable blobId, a probe that
  errors out, and a target without `urn:ietf:params:jmap:blob` are recorded as `retrievable`,
  `store_unavailable` and `unsupported` respectively, so a blob-store outage is never mistaken for
  N individually unrecoverable messages. The probe is not Email-only: a Sieve script and a file
  node go up through the very same content-addressed upload, so the shared `retry_if_blob_missing`
  now carries the pair of blobIds its retry observed back to the caller and a refusal that
  survives it is probed and classified the same way — a re-upload that returns the SAME blobId is
  the target itself saying it de-duplicated rather than stored. Object bytes are never perturbed;
  fidelity is unchanged (`jmap/blobxfer.rs`, `jmap/request.rs`, `sync/export.rs`,
  `sync/export/email.rs`, `sync/export/sieve.rs`, `sync/export/tree.rs`, `sync/export/uidtype.rs`).
- inspect: `vandelay inspect <ARCHIVE> --failures` dumps the quarantine, and the default summary
  gained a `failures` line so the state is discoverable. Deliberately a flag rather than a new
  `ObjectType` variant: `ObjectType::ALL` also drives which tables `export` walks, so a synthetic
  member would make export try to push the failure table at the target (`inspect/failures.rs`,
  `inspect/mod.rs`, `cli.rs`).
- inspect: `vandelay inspect <ARCHIVE> --failed-items --json` emits the quarantine as one JSON
  object, which is what makes the table usable by the control plane the rows were recorded for.
  The columnar dump is aligned text meant for a human; a caller parsing it was scraping widths
  and `(null)` placeholders, i.e. the same class of work the table replaced for stderr warnings.
  The envelope is `{"failed_items":[{"type":…,"id":…,"error":…,…}],"omitted":[…]}` with `type`,
  `id` and `error` present and non-empty on every row and every other field left out when the
  archive holds no value for it — an empty string is not a fact the archive recorded, and folding
  one into a stored value turns "no message-id was seen" into "the message-id is empty".
  `failed_items` is emitted even when the table is empty: that case is `{"failed_items":[]}` on
  stdout with exit 0, NOT the human "(no export failures in archive)" notice and not an empty
  body, because those are exactly what a binary without this surface produces and a caller must
  be able to tell "nothing failed" apart from "nothing implemented" — a half-built surface reading
  as a clean run is the outcome the quarantine exists to prevent. For the same reason the array is
  wrapped in the envelope key rather than emitted bare: another inspect mode could plausibly emit
  a bare array and the two are indistinguishable once parsed. `type` carries the JMAP type name of
  an object that names a single transferable item, and the match producing it is exhaustive over
  `ObjectType` so a future variant fails to compile here rather than surfacing in a consumer's
  parser as an unknown label. A refusal that is not a per-item one (a mailbox, an address book, a
  calendar, a file node) is withheld from `failed_items` and counted in `omitted` with its archive
  token and reason: passing it through makes a consumer reject the whole payload and lose the rows
  that ARE nameable, while dropping it unannounced hides a refusal. A row that cannot carry the
  three required fields ends the dump with an error instead of being shipped half-formed or
  skipped, since a caller has no way to tell either from a row it can act on. `--json` is rejected
  without the failure dump rather than ignored (`inspect/failures.rs`, `inspect/mod.rs`, `cli.rs`).

### Fixed
- import: `gc_orphan_blobs` reaped the bytes of a quarantined item. The GC runs at the end of every
  import and deletes any blob not referenced by `emails`/`sieve_scripts`/`file_nodes`/the `@blob`
  JSON sentinels; a message the target refused is frequently down to its last copy, so the next
  import would have destroyed exactly what the quarantine exists to preserve. `export_failures` is
  now part of the reachability set (`db/blobs.rs`).
- export: a quarantine row whose archive object had gone was never cleared. Rows are keyed by
  archive-local id, and those ids do not survive a re-import — an IMAP source reporting a changed
  UIDVALIDITY sends the coordinator through `wipe_folder_emails`, which deletes the folder's
  `emails` rows, and the re-import inserts the same messages again under fresh ids. The reconcile
  loop only visits ids it read back out of the archive, so it could never reach a row naming a
  dead one: the item stayed "unresolved" forever even after it had migrated fine, and because the
  row is part of the blob reachability set above, its `blob_local_id` pinned the deleted message's
  bytes against `gc_orphan_blobs` for the life of the archive — the table and the archive both grew
  without bound across import cycles. An export now sweeps rows with no surviving archive row
  before it reads the quarantine, for every type rather than just the run's work list: a type whose
  table has been emptied outright is dropped from that list by `has_rows`, which is exactly when
  all of its rows are stale (`db/export_failures.rs`, `sync/export.rs`).
- export: a per-item failure on the Sieve and address-book/calendar surfaces ended the whole pass
  instead of leaving a row. Both reconcilers let a single object's build leave by `?`: for a Sieve
  script that is the upload, the only fallible step there is, and for a collection it is reading
  and shaping the archive row — a `default_alerts_with_time` that is well-formed JSON of the wrong
  shape passes the table's CHECK and then fails to deserialize. `run()` caught the escape as `type
  SieveScript aborted`, charged the abort ONE `failed` unit, and moved on. Everything that made
  that number wrong was invisible: the unit named no object, so a caller reconciling `failed=N`
  against the quarantine found an empty table for a non-zero count and could only report a broken
  contract; and every object queued behind the broken one was never attempted at all, so the count
  under-reported the loss as badly as it failed to explain it — worst on Sieve, whose label the
  consuming vocabulary DOES carry, so a genuinely nameable refusal was the one being thrown away.
  Each is now recorded and skipped like every other per-item failure, with the same category names
  the Email path uses (`blobUploadFailed` for bytes that would not go up), and the Sieve row
  carries the script's own size and blake3 so the item is findable even though nothing reached the
  target. Only a whole-request failure — a `/set` call the transport could not complete — still
  aborts a type, which is what that sentinel means (`sync/export/sieve.rs`, `sync/export/flat.rs`).
- export: a folder merged onto a target of the same name kept its quarantine row forever. A local
  node that finds a name collision is mapped onto the existing target and counted as skipped, which
  is landing on the target every bit as much as creating it — but the merge short-circuits before
  any `/set`, so neither the `created` nor the `alreadyExists` branch that clears a row was
  reached, and `reap_missing` cannot help either: it only takes rows whose archive object is GONE,
  and this one is still there under the same local id. The row was therefore a permanent phantom
  that nothing in the lifecycle could ever clear — `inspect --failures` kept naming a folder that
  had migrated fine, a caller gating on "rows present == still unresolved" stayed blocked with no
  way out, and the row's `blob_local_id` kept pinning bytes against `gc_orphan_blobs`. Both merge
  paths (the batched tree and the interleaved file-node one) now resolve the row
  (`sync/export/tree.rs`).

### Carried
- Patch 1 (mp.1): exchange-graph read/unread (`$seen`) + flagged (`$flagged`) state on import.
- Patch 2 (mp.1): exchange-graph default Contacts folder import.
- Patch 3 (mp.2): refreshable `--access-token-file` so a long import survives token rotation.
- Patch 4 (mp.3): bounded exchange-graph import fetch concurrency.

## [1.0.7-mp.3] - 2026-07-19

MailPortal patched build (fork `kempu/vandelay`), rebased on stock `1.0.7` after the upstream
merge (PR #2). Carries the mp.1/mp.2 patches below plus an import-side memory fix. The binary
still reports version `1.0.7`; the patched build is distinguished by its tag/artifact (`-mp.3`)
and checksum.

### Fixed
- exchange-graph: bound message-body fetch concurrency on import (Patch 4). The fetch pool
  submitted every message id of a folder up front into an unbounded result channel, so on a
  large folder the fully-downloaded MIME bodies piled up in memory faster than the single
  SQLite writer — which periodically stalls on a WAL commit against a multi-GB archive — could
  drain them. That unbounded backlog grew with folder size and OOM-killed the process on a
  large mailbox (a 16 GB / 122k-message account). Now an in-flight WINDOW (topped up by one for
  each result drained) caps resident bodies regardless of folder size; job ids stay unbounded,
  only the heavy result side is throttled (`import_exchange_graph/messages.rs`).

### Carried
- Patch 1 (mp.1): exchange-graph read/unread (`$seen`) + flagged (`$flagged`) state on import.
- Patch 2 (mp.1): exchange-graph default Contacts folder import.
- Patch 3 (mp.2): refreshable `--access-token-file` so a long import survives token rotation.

## [1.0.7-mp.1] - 2026-07-13

MailPortal patched build (fork `kempu/vandelay`). Carries local patches on top of stock
`1.0.7` that are **not** upstream — the binary still reports version `1.0.7`; the patched
build is distinguished by its tag/artifact (`-mp.1`) and checksum. Re-check and re-apply on
every upstream bump; drop a patch once upstream fixes it. Full rationale + re-apply protocol
live in the MailPortal repo `docs/vandelay-patches.md`.

### Fixed
- exchange-graph: preserve message read/unread (`$seen`) and flagged (`$flagged`) state on
  import. Stock derived keywords only from the MIME blob, which does not carry these Graph
  message properties, so every migrated message landed unread and unflagged (Patch 1).
- exchange-graph: import the DEFAULT Contacts folder (`{me_or_user}/contacts`), which the
  `/contactFolders` collection excludes. Stock silently imported zero contacts for a mailbox
  whose contacts live in the default folder — i.e. most mailboxes (Patch 2).

## [1.0.7] - 2026-07-XX

### Added

### Changed

### Fixed
- Improve verbosity (#4 #19).
- WebDAV import materialised the account root collection as a directory named after the account displayname (#18).
- Report user friendly error message when `urn:ietf:params:jmap:principals` is not supported and no accountId is provided (#21).
- Report which email failed to import when the blob is too large (#22).

## [1.0.6] - 2026-07-12

### Added

### Changed

### Fixed
- Self heal on `blobNotFound` errors when exporting data (#13).
- Mapping existing special mailbox fails after `alreadyExists` response (#17).

## [1.0.5] - 2026-06-27

### Added

### Changed

### Fixed
- Strict `RFC822.SIZE` == `BODY[]` length check discards good mail.

## [1.0.4] - 2026-06-21

### Added

### Changed

### Fixed
- Include correct JMAP capabilities in `using`.
- Failures are double-counted.

## [1.0.3] - 2026-06-15

### Added

### Changed

### Fixed
- Mailbox roles must be unique per archive (#8).
- Google takeout: Decode MIME-encoded values in `X-Gmail-Labels` (#7).

## [1.0.2] - 2026-06-11

### Added

### Changed

### Fixed
- IMAP: Import fails with `BAD` on servers that advertise `LIST-EXTENDED` without `SPECIAL-USE`.
- MS Exchange EWS: add support for version negotiation and other fixes (#6).

## [1.0.1] - 2026-06-04

### Added

### Changed

### Fixed
- MS Exchange Graph: duplicate ids and incorrect JSCalendar mapping issues.

## [1.0.0] - 2026-05-29

### Added
- Initial release.

### Changed

### Fixed
