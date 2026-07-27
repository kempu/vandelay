/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Per-item export quarantine.
//!
//! Export used to report failures only as aggregate counters on stdout
//! (`Email: created=.. failed=N`) plus unstructured warnings on stderr, so a
//! control plane driving the run could learn that N items were refused but
//! never WHICH ones. This table is the structured counterpart: one row per
//! object the target refused, carrying enough identity (message-id, size,
//! content hash, the blobId the target handed us) to chase the item down on
//! the server afterwards.
//!
//! The primary key is `(type_name, local_id)`, so a re-run REPLACES the row
//! rather than appending: the table always reflects the latest attempt and
//! never grows into an unbounded log. `resolve()` deletes the row once the
//! item makes it onto the target, so "rows present" means "still unresolved".
//! `resolve()` cannot hold that invariant up on its own — it is only ever
//! reached for ids the exporter still finds in the archive — so `reap_missing()`
//! covers the rows it structurally cannot reach.
//!
//! `type_name` deliberately carries no CHECK constraint. The values come from
//! `ObjectType::token()` and constraining them here would turn a future
//! ObjectType addition into a runtime write failure mid-export.
//!
//! `blob_local_id` is a plain INTEGER, not a foreign key. A quarantine row has
//! to outlive the archive row it describes — importers delete and re-insert
//! `emails` rows on a re-import, and `foreign_keys = ON` would abort that with
//! an FK violation (or, with CASCADE, silently drop the quarantine record).
//! It is still honoured by `blobs::gc_orphan_blobs`, which keeps the bytes of
//! a quarantined item alive even when nothing else references them.

use rusqlite::{Connection, params};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// No probe ran: the failure was not a `blobNotFound` that survived a re-upload.
pub const PROBE_NOT_PROBED: &str = "not_probed";
/// The target could serve the blob back, so `blobNotFound` came from elsewhere.
pub const PROBE_RETRIEVABLE: &str = "retrievable";
/// Upload returned 200 with a blobId the target then reports as not found: a
/// content-hash de-duplication marker shadowing bytes that are gone from the
/// blob store. Re-uploading identical bytes cannot fix this — the same bytes
/// hash the same and are de-duped away again.
pub const PROBE_ORPHANED_MARKER: &str = "orphaned_marker";
/// The lookup itself failed (transport, 5xx, retries exhausted). The blob store
/// is unhealthy; a whole run's worth of rows in this state is a systemic
/// outage, not N independently quarantined messages.
pub const PROBE_STORE_UNAVAILABLE: &str = "store_unavailable";
/// The target does not advertise `urn:ietf:params:jmap:blob`, so retrievability
/// cannot be established. Recorded as-is rather than guessed at.
pub const PROBE_UNSUPPORTED: &str = "unsupported";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedItem {
    pub type_name: String,
    pub local_id: i64,
    pub client_id: String,
    pub message_id: Option<String>,
    pub size_bytes: Option<i64>,
    pub blob_local_id: Option<i64>,
    pub blob_hash: Option<String>,
    pub target_blob_id: Option<String>,
    pub error_type: String,
    pub error_detail: String,
    pub blob_probe: String,
    pub failed_at: String,
}

const SELECT_COLUMNS: &str = "type_name, local_id, client_id, message_id, size_bytes,
     blob_local_id, blob_hash, target_blob_id, error_type, error_detail, blob_probe, failed_at";

pub fn record(conn: &Connection, item: &FailedItem) -> Result<(), rusqlite::Error> {
    conn.execute(
        "INSERT OR REPLACE INTO export_failures
             (type_name, local_id, client_id, message_id, size_bytes, blob_local_id,
              blob_hash, target_blob_id, error_type, error_detail, blob_probe, failed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            item.type_name,
            item.local_id,
            item.client_id,
            item.message_id,
            item.size_bytes,
            item.blob_local_id,
            item.blob_hash,
            item.target_blob_id,
            item.error_type,
            item.error_detail,
            item.blob_probe,
            item.failed_at,
        ],
    )?;
    Ok(())
}

pub fn resolve(conn: &Connection, type_name: &str, local_id: i64) -> Result<(), rusqlite::Error> {
    conn.execute(
        "DELETE FROM export_failures WHERE type_name = ?1 AND local_id = ?2",
        params![type_name, local_id],
    )?;
    Ok(())
}

/// Drop the rows of `type_name` whose object is no longer in the archive.
///
/// A row is keyed by an archive-local id, and those ids do not survive a
/// re-import: an IMAP source reporting a changed UIDVALIDITY sends the
/// coordinator through `wipe_folder_emails`, which deletes every `emails` row of
/// the folder, and the re-import inserts the same messages again under fresh
/// ids. Every row this table was holding then names an id that no longer exists,
/// and nothing in the export reconcile loop can clear it — that loop only visits
/// ids it read back out of the archive, so `resolve()` is never called for a
/// dangling one. Left in place such a row is a permanent phantom: `inspect
/// --failures` keeps reporting an item that has since migrated fine, a caller
/// gating on "rows present == still unresolved" is blocked with no way to clear
/// it, and the row's `blob_local_id` keeps pinning the message bytes against
/// `blobs::gc_orphan_blobs` for the life of the archive. That is what bounds the
/// deliberate absence of a foreign key above: a quarantine row must outlive the
/// archive row it describes, but not outlive it forever.
///
/// Sweeping is safe precisely because the archive is opened with
/// `locking_mode = EXCLUSIVE`: no import can be halfway through deleting and
/// re-inserting its rows while this runs, so a missing object is a settled fact
/// and not a race. Nothing is lost either — if the item is still in the archive
/// under its new id and still fails, the same run records it again under that id.
///
/// `table` is interpolated because SQLite cannot bind an identifier; callers
/// pass `sync::table_name()`, a closed match over `ObjectType`.
pub fn reap_missing(
    conn: &Connection,
    type_name: &str,
    table: &str,
) -> Result<usize, rusqlite::Error> {
    conn.execute(
        &format!(
            "DELETE FROM export_failures
             WHERE type_name = ?1 AND local_id NOT IN (SELECT id FROM {table})"
        ),
        params![type_name],
    )
}

pub fn local_ids(conn: &Connection, type_name: &str) -> Result<Vec<i64>, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT local_id FROM export_failures WHERE type_name = ?1")?;
    let rows = stmt.query_map(params![type_name], |row| row.get(0))?;
    rows.collect()
}

pub fn count(conn: &Connection) -> Result<u64, rusqlite::Error> {
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM export_failures", [], |row| row.get(0))?;
    Ok(n.max(0) as u64)
}

pub fn list(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
) -> Result<Vec<FailedItem>, rusqlite::Error> {
    let page = match limit {
        Some(n) => format!(" LIMIT {n} OFFSET {offset}"),
        None if offset > 0 => format!(" LIMIT -1 OFFSET {offset}"),
        None => String::new(),
    };
    let sql =
        format!("SELECT {SELECT_COLUMNS} FROM export_failures ORDER BY type_name, local_id{page}");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(FailedItem {
            type_name: row.get(0)?,
            local_id: row.get(1)?,
            client_id: row.get(2)?,
            message_id: row.get(3)?,
            size_bytes: row.get(4)?,
            blob_local_id: row.get(5)?,
            blob_hash: row.get(6)?,
            target_blob_id: row.get(7)?,
            error_type: row.get(8)?,
            error_detail: row.get(9)?,
            blob_probe: row.get(10)?,
            failed_at: row.get(11)?,
        })
    })?;
    rows.collect()
}

/// Wall-clock stamp for a quarantine row: RFC 3339 in UTC.
///
/// `format` is fallible and this deliberately does not paper over that. RFC 3339
/// refuses exactly three things — a calendar year outside `0..=9999`, an offset
/// hour beyond ±23, and a non-zero offset second — and `now_utc()` can present
/// none of them: its offset is UTC by construction, and a clock far enough into
/// the future to leave the year range panics inside `now_utc()` itself, `Date`
/// topping out at year 9999 without the `large-dates` feature. What is left is a
/// host clock set before year 0, which makes every other timestamp the run
/// writes fiction as well. Substituting a plausible constant there (the epoch,
/// say) would be the worst of the options: `failed_at` is what a control plane
/// sorts and ages quarantined items by, so a fabricated value reads exactly like
/// a real one and the reader has no way to tell them apart.
pub fn stamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("now_utc() is UTC and inside the RFC 3339 year range")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init::apply_schema(&c).unwrap();
        c
    }

    fn item(local_id: i64, detail: &str) -> FailedItem {
        FailedItem {
            type_name: "email".to_owned(),
            local_id,
            client_id: format!("e{local_id}"),
            message_id: Some("m-1@h".to_owned()),
            size_bytes: Some(42),
            blob_local_id: Some(7),
            blob_hash: Some("abcd".to_owned()),
            target_blob_id: Some("UP1".to_owned()),
            error_type: "blobNotFound".to_owned(),
            error_detail: detail.to_owned(),
            blob_probe: PROBE_ORPHANED_MARKER.to_owned(),
            failed_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn record_then_list_roundtrips_every_column() {
        let c = mem();
        let original = item(23, "first attempt");
        record(&c, &original).unwrap();
        let rows = list(&c, None, 0).unwrap();
        assert_eq!(rows, vec![original]);
    }

    #[test]
    fn rerun_replaces_the_row_instead_of_appending() {
        let c = mem();
        record(&c, &item(23, "first attempt")).unwrap();
        record(&c, &item(23, "second attempt")).unwrap();
        let rows = list(&c, None, 0).unwrap();
        assert_eq!(rows.len(), 1, "one row per (type, local id): {rows:?}");
        assert_eq!(rows[0].error_detail, "second attempt");
        assert_eq!(count(&c).unwrap(), 1);
    }

    #[test]
    fn distinct_local_ids_keep_distinct_rows() {
        let c = mem();
        record(&c, &item(23, "a")).unwrap();
        record(&c, &item(24, "b")).unwrap();
        assert_eq!(count(&c).unwrap(), 2);
        assert_eq!(local_ids(&c, "email").unwrap(), vec![23, 24]);
        assert!(local_ids(&c, "sievescript").unwrap().is_empty());
    }

    #[test]
    fn resolve_removes_the_row_once_the_item_lands_on_target() {
        let c = mem();
        record(&c, &item(23, "a")).unwrap();
        resolve(&c, "email", 23).unwrap();
        assert_eq!(count(&c).unwrap(), 0);
        resolve(&c, "email", 23).unwrap();
    }

    /// Give `local_id` an `emails` row so a sweep has something to keep.
    fn seed_email(c: &Connection, local_id: i64) {
        let blob = crate::db::blobs::intern_blob(c, format!("msg {local_id}").as_bytes()).unwrap();
        c.execute(
            "INSERT INTO emails (id, blob_id, received_at, mailbox_ids, keywords)
             VALUES (?1, ?2, '2020-01-01T00:00:00Z', '[1]', '[]')",
            params![local_id, blob],
        )
        .unwrap();
    }

    #[test]
    fn reap_drops_rows_whose_archive_object_is_gone() {
        let c = mem();
        seed_email(&c, 23);
        record(&c, &item(23, "still in the archive")).unwrap();
        record(&c, &item(24, "wiped by a re-import")).unwrap();
        assert_eq!(reap_missing(&c, "email", "emails").unwrap(), 1);
        assert_eq!(
            local_ids(&c, "email").unwrap(),
            vec![23],
            "only the row nothing can ever resolve is swept"
        );
    }

    #[test]
    fn reap_only_touches_the_type_it_is_given() {
        let c = mem();
        record(&c, &item(23, "a")).unwrap();
        // Not one `mailboxes` row exists, so a careless sweep keyed on the wrong
        // type would take the email rows with it.
        assert_eq!(reap_missing(&c, "mailbox", "mailboxes").unwrap(), 0);
        assert_eq!(local_ids(&c, "email").unwrap(), vec![23]);
    }

    #[test]
    fn reap_is_a_no_op_once_the_table_is_clean() {
        let c = mem();
        seed_email(&c, 23);
        record(&c, &item(23, "a")).unwrap();
        assert_eq!(reap_missing(&c, "email", "emails").unwrap(), 0);
        assert_eq!(reap_missing(&c, "email", "emails").unwrap(), 0);
        assert_eq!(count(&c).unwrap(), 1);
    }

    #[test]
    fn unknown_probe_state_is_rejected_by_the_schema() {
        let c = mem();
        let mut bad = item(23, "a");
        bad.blob_probe = "made-up".to_owned();
        assert!(record(&c, &bad).is_err(), "CHECK constraint must hold");
    }

    #[test]
    fn stamp_is_rfc3339_utc_and_comes_from_the_clock() {
        let before = OffsetDateTime::now_utc();
        let s = stamp_now();
        assert!(s.ends_with('Z'), "got {s}");
        // Parsing alone would also accept a substituted constant, which is what
        // this stamp must never be: a `failed_at` that silently stands in for a
        // formatting failure is indistinguishable from a genuine one downstream.
        match OffsetDateTime::parse(&s, &Rfc3339) {
            Ok(t) => assert!(t >= before, "stamp predates the call: {s}"),
            Err(e) => panic!("not RFC 3339: {s} ({e})"),
        }
    }
}
