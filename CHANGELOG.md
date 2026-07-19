# Change Log

All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

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
