/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Read-back surface for the per-item export quarantine.
//!
//! The aggregate counters an export prints on stdout say how many items the
//! target refused; they cannot say which. This dump answers that question from
//! the archive itself, so a caller driving the migration never has to scrape
//! warnings out of stderr to find out what was left behind.
//!
//! Note that the archive is opened with `locking_mode = EXCLUSIVE`, so this is
//! readable only once the exporting process has released it — i.e. between
//! runs, which is exactly when the answer is wanted.
//!
//! Two renderings of the same rows live here. `write_failures` is the columnar
//! dump an operator reads; `write_failures_json` is the machine contract a
//! control plane parses. They are separate functions rather than one function
//! with a format switch woven through it because the columnar layout is free to
//! change — column widths, wording, an added hint line — while the JSON envelope
//! is pinned by a caller that is deployed independently of this binary.

use std::io::Write;

use rusqlite::Connection;
use serde_json::{Map, Value, json};

use super::list::write_record;
use super::{format_bytes, format_count};
use crate::db::export_failures;
use crate::db::export_failures::FailedItem;
use crate::error::Error;
use crate::types::ObjectType;

pub fn write_failures(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
) -> Result<(), Error> {
    let total = export_failures::count(conn)?;
    write_header(out, total, limit, offset)?;
    if total == 0 {
        writeln!(out, "(no export failures in archive)")?;
        return Ok(());
    }
    let rows = export_failures::list(conn, limit, offset)?;
    let written = rows.len() as u64;
    for row in rows {
        let fields = vec![
            ("client_id", row.client_id),
            ("message_id", row.message_id.unwrap_or_else(no_value)),
            (
                "size",
                row.size_bytes.map(bytes_or_none).unwrap_or_else(no_value),
            ),
            ("blob", blob_field(row.blob_local_id, row.blob_hash)),
            (
                "target_blob_id",
                row.target_blob_id.unwrap_or_else(no_value),
            ),
            ("error_type", row.error_type),
            ("blob_probe", row.blob_probe),
            ("error_detail", row.error_detail),
            ("failed_at", row.failed_at),
        ];
        write_record(
            out,
            &format!("{} #{}", row.type_name, row.local_id),
            &fields,
        )?;
    }
    if let Some(lim) = limit {
        let next_offset = (offset as u64) + written;
        if next_offset < total {
            writeln!(
                out,
                "... ({} more; --offset {} --limit {} to continue)",
                format_count(total - next_offset),
                next_offset,
                lim
            )?;
        }
    }
    Ok(())
}

/// Emit the quarantine as a single JSON object on one line.
///
/// The envelope is the contract, not a convenience:
///
/// ```json
/// {"failed_items":[{"type":"Email","id":"e499","message_id":"<a@example.com>",
///   "size":73932,"content_hash":"…","blob_id":"ebn0","error":"blobNotFound",
///   "detail":"BlobId ebn0 not found."}],"omitted":[]}
/// ```
///
/// `failed_items` is ALWAYS present and is always an array, including when the
/// archive holds no failures at all — that case emits `{"failed_items":[]}` and
/// exits 0, never the columnar "(no export failures in archive)" notice and
/// never an empty stdout. The caller deliberately treats empty output and a
/// missing envelope key as errors rather than as "nothing failed": those are
/// exactly what a binary that does not implement this surface produces, and a
/// half-implemented surface reading as a clean run is the one outcome this
/// whole table exists to prevent. Emitting the bare array without the envelope
/// key would be the same trap in a subtler form — some other inspect mode could
/// plausibly emit a bare array, and the two are indistinguishable once parsed.
///
/// `type`, `id` and `error` are present and non-empty on every row; every other
/// field is omitted when the archive holds no value for it, which the caller
/// reads as null. A row that cannot satisfy the three required fields is not
/// emitted half-formed and not skipped either — it ends the dump with an error,
/// because both of the alternatives hand the caller a list it will believe.
///
/// An archive written before `export_failures` existed needs no special case:
/// `db::init::open` runs the whole schema on every open and the table is created
/// `IF NOT EXISTS`, so an old archive gets an empty table and reports `[]` here
/// rather than failing on a missing relation.
pub fn write_failures_json(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
) -> Result<(), Error> {
    let rows = export_failures::list(conn, limit, offset)?;
    let mut items = Vec::with_capacity(rows.len());
    let mut omitted: Vec<(String, &'static str, u64)> = Vec::new();
    for row in rows {
        match item_label(&row.type_name) {
            Ok(label) => items.push(json_row(label, row)?),
            Err(reason) => note_omitted(&mut omitted, row.type_name, reason),
        }
    }
    let envelope = json!({
        "failed_items": Value::Array(items),
        "omitted": omitted
            .into_iter()
            .map(|(type_name, reason, count)| json!({
                "type": type_name,
                "reason": reason,
                "count": count,
            }))
            .collect::<Vec<Value>>(),
    });
    // `Display` for `Value` is the compact single-line form and cannot fail;
    // only the write to `out` can, and that is propagated.
    writeln!(out, "{envelope}")?;
    Ok(())
}

/// A row of a type the structured contract cannot name is withheld from
/// `failed_items` and counted here instead.
///
/// It is NOT dropped on the floor. The consumer's vocabulary covers the object
/// types that name a single transferable item; a container refusal (a mailbox,
/// an address book, a calendar, a file node) is a different kind of event, is
/// already visible in the run's own `failed=N` summary line, and has no token in
/// that vocabulary — feeding it in anyway makes the consumer reject the whole
/// payload, which loses the per-item rows that ARE nameable along with it. So
/// the omission is stated in the payload, with the raw archive token and a
/// reason, rather than being something a reader has to infer from a short list.
fn note_omitted(
    omitted: &mut Vec<(String, &'static str, u64)>,
    type_name: String,
    reason: &'static str,
) {
    if let Some(entry) = omitted.iter_mut().find(|(t, _, _)| *t == type_name) {
        entry.2 += 1;
        return;
    }
    omitted.push((type_name, reason, 1));
}

const NOT_AN_ITEM_TYPE: &str =
    "not a per-item object type: a refusal here does not name a single transferable item";
const UNKNOWN_TYPE: &str = "object type unknown to this binary: archive written by another build";

/// The `type` label a structured row carries, or the reason it cannot have one.
///
/// The label is `ObjectType::jmap_name()` — the spelling the protocol itself
/// uses — and the match over `ObjectType` is exhaustive on purpose: adding a
/// variant then fails to compile here, which forces whoever adds it to decide
/// whether the new type names an item this contract can carry instead of having
/// it appear in a caller's parser as an unknown label at runtime.
fn item_label(type_name: &str) -> Result<&'static str, &'static str> {
    let Ok(ty) = ObjectType::parse(type_name) else {
        return Err(UNKNOWN_TYPE);
    };
    match ty {
        ObjectType::Email
        | ObjectType::ContactCard
        | ObjectType::CalendarEvent
        | ObjectType::SieveScript
        | ObjectType::Identity => Ok(ty.jmap_name()),
        ObjectType::Mailbox
        | ObjectType::AddressBook
        | ObjectType::Calendar
        | ObjectType::FileNode
        // ParticipantIdentity is a leaf in this archive but is not a surface the
        // structured contract names, and nothing quarantines one today: it is
        // reconciled by `keyed`, which keeps no quarantine at all.
        | ObjectType::ParticipantIdentity => Err(NOT_AN_ITEM_TYPE),
    }
}

/// Render one quarantine row, or fail if it cannot carry the required fields.
///
/// `blob_probe` and `failed_at` are carried too even though the documented
/// envelope does not require them: the columnar dump shows both, and a machine
/// surface that is poorer than the human one sends its reader back to scraping
/// text for the very facts — is this an orphaned de-duplication marker or a
/// blob store outage, and how old is the row — that decide what to do about the
/// item. Extra keys are inert for a caller that reads only what it declared.
fn json_row(label: &'static str, row: FailedItem) -> Result<Value, Error> {
    if row.client_id.is_empty() {
        return Err(Error::Partial(format!(
            "quarantine row {} #{} carries no creation id; the structured dump cannot name it",
            row.type_name, row.local_id
        )));
    }
    if row.error_type.is_empty() {
        return Err(Error::Partial(format!(
            "quarantine row {} #{} carries no error type; the structured dump cannot classify it",
            row.type_name, row.local_id
        )));
    }

    let mut obj = Map::new();
    obj.insert("type".to_owned(), Value::String(label.to_owned()));
    obj.insert("id".to_owned(), Value::String(row.client_id));
    insert_present(&mut obj, "message_id", row.message_id);
    if let Some(size) = row.size_bytes {
        obj.insert("size".to_owned(), json!(size));
    }
    insert_present(&mut obj, "content_hash", row.blob_hash);
    insert_present(&mut obj, "blob_id", row.target_blob_id);
    obj.insert("error".to_owned(), Value::String(row.error_type));
    insert_present(&mut obj, "detail", Some(row.error_detail));
    obj.insert("blob_probe".to_owned(), Value::String(row.blob_probe));
    obj.insert("failed_at".to_owned(), Value::String(row.failed_at));
    Ok(Value::Object(obj))
}

/// Write an optional column only when the archive actually holds a value.
///
/// An empty string is treated as absent rather than emitted: it is not a fact
/// the archive recorded, and a caller folding `""` into a stored value would
/// turn "no message-id was seen" into "the message-id is the empty string".
fn insert_present(obj: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(v) = value
        && !v.is_empty()
    {
        obj.insert(key.to_owned(), Value::String(v));
    }
}

fn write_header(
    out: &mut impl Write,
    total: u64,
    limit: Option<usize>,
    offset: usize,
) -> Result<(), Error> {
    match limit {
        None if offset == 0 => writeln!(out, "Export failures ({})", format_count(total))?,
        None => writeln!(
            out,
            "Export failures ({}, skipping first {})",
            format_count(total),
            format_count(offset as u64)
        )?,
        Some(lim) => writeln!(
            out,
            "Export failures ({} total, showing up to {} from offset {})",
            format_count(total),
            format_count(lim as u64),
            format_count(offset as u64)
        )?,
    }
    writeln!(out)?;
    Ok(())
}

fn no_value() -> String {
    "(null)".to_owned()
}

fn bytes_or_none(n: i64) -> String {
    format_bytes(n.max(0) as u64)
}

// The full blake3 is printed, not the 12-char prefix `blob_summary` uses for
// listings: on a content-addressed target it is the key the server de-duplicates
// on, so it is what lets an operator match a quarantined item against the marker
// the server is still holding.
//
// A null hash is the absence of a recorded fact, not evidence that the bytes
// went away: export records a null both when the archive genuinely no longer
// holds them and when the read that would have produced the hash failed (a case
// it also spells out in `error_detail`). This must therefore not assert the
// stronger of the two readings — an operator deciding whether a quarantined
// message can still be re-sent would be acting on a claim the row cannot make.
fn blob_field(local_id: Option<i64>, hash: Option<String>) -> String {
    match (local_id, hash) {
        (Some(id), Some(hex)) => format!("local #{id} blake3={hex}"),
        (Some(id), None) => format!("local #{id} (no content hash recorded)"),
        (None, Some(hex)) => format!("blake3={hex}"),
        (None, None) => no_value(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::export_failures::{FailedItem, PROBE_NOT_PROBED, PROBE_ORPHANED_MARKER};
    use crate::db::init;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init::apply_schema(&c).unwrap();
        c
    }

    fn item(local_id: i64, probe: &str) -> FailedItem {
        FailedItem {
            type_name: "email".to_owned(),
            local_id,
            client_id: format!("e{local_id}"),
            message_id: Some("m-1@h".to_owned()),
            size_bytes: Some(2048),
            blob_local_id: Some(7),
            blob_hash: Some("a".repeat(64)),
            target_blob_id: Some("UP1".to_owned()),
            error_type: "blobNotFound".to_owned(),
            error_detail: r#"{"type":"blobNotFound"}"#.to_owned(),
            blob_probe: probe.to_owned(),
            failed_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn empty_table_prints_none_message() {
        let c = mem();
        let mut buf = Vec::new();
        write_failures(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Export failures (0)"), "got:\n{s}");
        assert!(s.contains("(no export failures in archive)"));
    }

    #[test]
    fn record_dump_carries_every_field_the_caller_needs() {
        let c = mem();
        export_failures::record(&c, &item(23, PROBE_ORPHANED_MARKER)).unwrap();
        let mut buf = Vec::new();
        write_failures(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Export failures (1)"), "got:\n{s}");
        assert!(s.contains("email #23"), "got:\n{s}");
        assert!(s.contains("client_id       e23"), "got:\n{s}");
        assert!(s.contains("message_id      m-1@h"), "got:\n{s}");
        assert!(s.contains("size            2.0 KB"), "got:\n{s}");
        assert!(
            s.contains(&format!("blake3={}", "a".repeat(64))),
            "got:\n{s}"
        );
        assert!(s.contains("target_blob_id  UP1"), "got:\n{s}");
        assert!(s.contains("error_type      blobNotFound"), "got:\n{s}");
        assert!(s.contains("blob_probe      orphaned_marker"), "got:\n{s}");
        assert!(s.contains("failed_at       2026-01-01"), "got:\n{s}");
    }

    #[test]
    fn absent_optional_columns_render_as_null() {
        let c = mem();
        let mut sparse = item(1, PROBE_NOT_PROBED);
        sparse.message_id = None;
        sparse.size_bytes = None;
        sparse.target_blob_id = None;
        export_failures::record(&c, &sparse).unwrap();
        let mut buf = Vec::new();
        write_failures(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("message_id      (null)"), "got:\n{s}");
        assert!(s.contains("size            (null)"), "got:\n{s}");
        assert!(s.contains("target_blob_id  (null)"), "got:\n{s}");
    }

    // A null hash says the export had no hash to record. It does not establish
    // that the archive lost the bytes — export also writes a null when the read
    // that would have produced the hash failed — and an operator deciding
    // whether a quarantined message can still be re-sent must not be told the
    // stronger thing.
    #[test]
    fn a_row_without_a_hash_does_not_claim_the_bytes_are_gone() {
        let c = mem();
        let mut hashless = item(9, PROBE_NOT_PROBED);
        hashless.blob_hash = None;
        export_failures::record(&c, &hashless).unwrap();
        let mut buf = Vec::new();
        write_failures(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains("blob            local #7 (no content hash recorded)"),
            "got:\n{s}"
        );
        assert!(!s.contains("no longer in archive"), "got:\n{s}");
    }

    fn json_of(c: &Connection, limit: Option<usize>, offset: usize) -> Value {
        let mut buf = Vec::new();
        write_failures_json(c, limit, offset, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.ends_with('\n'), "envelope is one line: {s:?}");
        assert_eq!(s.lines().count(), 1, "envelope is one line: {s:?}");
        serde_json::from_str(&s).unwrap_or_else(|e| panic!("not JSON: {s} ({e})"))
    }

    #[test]
    fn json_mode_emits_the_documented_envelope() {
        let c = mem();
        export_failures::record(&c, &item(499, PROBE_ORPHANED_MARKER)).unwrap();
        let envelope = json_of(&c, None, 0);
        let items = envelope["failed_items"].as_array().expect("failed_items");
        assert_eq!(items.len(), 1);
        let row = &items[0];
        assert_eq!(row["type"], json!("Email"));
        assert_eq!(row["id"], json!("e499"));
        assert_eq!(row["message_id"], json!("m-1@h"));
        assert_eq!(row["size"], json!(2048));
        assert_eq!(row["content_hash"], json!("a".repeat(64)));
        assert_eq!(row["blob_id"], json!("UP1"));
        assert_eq!(row["error"], json!("blobNotFound"));
        assert_eq!(row["detail"], json!(r#"{"type":"blobNotFound"}"#));
        assert_eq!(row["blob_probe"], json!("orphaned_marker"));
        assert_eq!(row["failed_at"], json!("2026-01-01T00:00:00Z"));
    }

    // The label has to be one the consumer's own vocabulary maps: it lowercases
    // before looking it up, and a label it does not know makes it reject the
    // payload rather than skip the row.
    #[test]
    fn every_item_type_label_folds_to_a_lowercase_archive_token() {
        for ty in ObjectType::ALL {
            if let Ok(label) = item_label(ty.token()) {
                assert_eq!(label.to_ascii_lowercase(), ty.token(), "label {label}");
            }
        }
        assert_eq!(item_label("email"), Ok("Email"));
        assert_eq!(item_label("sievescript"), Ok("SieveScript"));
        assert_eq!(item_label("contactcard"), Ok("ContactCard"));
        assert_eq!(item_label("calendarevent"), Ok("CalendarEvent"));
    }

    // Empty stdout and the human "(no export failures in archive)" notice are
    // both read by the caller as a broken surface, deliberately, so that a
    // binary which does not implement this dump can never look like a clean run.
    #[test]
    fn zero_rows_emit_the_empty_envelope_not_the_human_notice() {
        let c = mem();
        let mut buf = Vec::new();
        write_failures_json(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.trim_end(), r#"{"failed_items":[],"omitted":[]}"#);
        assert!(!s.contains("no export failures"), "got:\n{s}");
        assert!(!s.contains("Export failures"), "got:\n{s}");
        let envelope = json_of(&c, None, 0);
        assert_eq!(envelope["failed_items"], json!([]));
    }

    // The archive predates the quarantine table. `init::open` applies the whole
    // schema on every open and every statement in it is `IF NOT EXISTS`, so the
    // table is created on the spot instead of the dump failing on a missing
    // relation — which is what an operator inspecting an old archive hits.
    #[test]
    fn an_archive_written_before_the_table_existed_reports_no_failures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.sqlite3");
        {
            let old = Connection::open(&path).unwrap();
            old.execute_batch(
                "CREATE TABLE emails (id INTEGER PRIMARY KEY, blob_id INTEGER,
                     received_at TEXT, mailbox_ids TEXT, keywords TEXT)",
            )
            .unwrap();
        }
        let c = init::open(&path).unwrap();
        assert_eq!(json_of(&c, None, 0)["failed_items"], json!([]));
    }

    // A container refusal has no token in the consumer's vocabulary. Passing it
    // through anyway makes the consumer reject the entire payload, taking the
    // per-item rows with it; dropping it without saying so hides a refusal the
    // rows exist to explain. So it is withheld and named.
    #[test]
    fn a_container_row_is_withheld_from_items_and_named_in_omitted() {
        let c = mem();
        export_failures::record(&c, &item(1, PROBE_NOT_PROBED)).unwrap();
        let mut mailbox = item(2, PROBE_NOT_PROBED);
        mailbox.type_name = "mailbox".to_owned();
        export_failures::record(&c, &mailbox).unwrap();
        let mut other_mailbox = item(3, PROBE_NOT_PROBED);
        other_mailbox.type_name = "mailbox".to_owned();
        export_failures::record(&c, &other_mailbox).unwrap();

        let envelope = json_of(&c, None, 0);
        let items = envelope["failed_items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "only the item-typed row: {items:?}");
        assert_eq!(items[0]["type"], json!("Email"));
        let omitted = envelope["omitted"].as_array().unwrap();
        assert_eq!(omitted.len(), 1, "one entry per type: {omitted:?}");
        assert_eq!(omitted[0]["type"], json!("mailbox"));
        assert_eq!(omitted[0]["count"], json!(2));
        assert!(
            omitted[0]["reason"].as_str().unwrap().contains("per-item"),
            "got: {omitted:?}"
        );
    }

    // A null column is the absence of a recorded fact, so the key is left out
    // rather than emitted as an empty string the caller would store as a value.
    #[test]
    fn absent_optional_columns_are_omitted_from_the_object() {
        let c = mem();
        let mut sparse = item(1, PROBE_NOT_PROBED);
        sparse.message_id = None;
        sparse.size_bytes = None;
        sparse.blob_hash = None;
        sparse.target_blob_id = None;
        export_failures::record(&c, &sparse).unwrap();
        let envelope = json_of(&c, None, 0);
        let row = envelope["failed_items"][0].as_object().unwrap();
        for absent in ["message_id", "size", "content_hash", "blob_id"] {
            assert!(
                !row.contains_key(absent),
                "{absent} should be absent: {row:?}"
            );
        }
        // The three the contract requires are there regardless.
        assert_eq!(row["type"], json!("Email"));
        assert_eq!(row["id"], json!("e1"));
        assert_eq!(row["error"], json!("blobNotFound"));
    }

    // Half a row is worse than an error: the caller has no way to tell a row it
    // cannot identify from one it can, so it would quarantine a nameless item.
    #[test]
    fn a_row_without_a_creation_id_fails_the_dump_rather_than_shipping_half_of_it() {
        let c = mem();
        let mut nameless = item(7, PROBE_NOT_PROBED);
        nameless.client_id = String::new();
        export_failures::record(&c, &nameless).unwrap();
        let mut buf = Vec::new();
        match write_failures_json(&c, None, 0, &mut buf) {
            Err(Error::Partial(m)) => assert!(m.contains("creation id"), "msg was: {m}"),
            other => panic!("expected a Partial error, got {other:?}"),
        }
    }

    #[test]
    fn json_mode_honours_limit_and_offset() {
        let c = mem();
        for i in 1..=5 {
            export_failures::record(&c, &item(i, PROBE_NOT_PROBED)).unwrap();
        }
        let ids: Vec<&str> = vec!["e3", "e4"];
        let envelope = json_of(&c, Some(2), 2);
        let got: Vec<&str> = envelope["failed_items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect();
        assert_eq!(got, ids);
    }

    // The columnar dump is what an operator reads and is not a JSON document;
    // --json is an addition to this surface, not a change to it.
    #[test]
    fn human_mode_is_untouched_by_the_json_surface() {
        let c = mem();
        export_failures::record(&c, &item(23, PROBE_ORPHANED_MARKER)).unwrap();
        let mut buf = Vec::new();
        write_failures(&c, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(!s.contains("failed_items"), "got:\n{s}");
        assert!(s.starts_with("Export failures (1)"), "got:\n{s}");
        assert!(s.contains("email #23"), "got:\n{s}");
        assert!(
            serde_json::from_str::<Value>(&s).is_err(),
            "the operator dump is text, not JSON:\n{s}"
        );
    }

    #[test]
    fn pagination_writes_continuation_hint_when_more_remain() {
        let c = mem();
        for i in 1..=5 {
            export_failures::record(&c, &item(i, PROBE_NOT_PROBED)).unwrap();
        }
        let mut buf = Vec::new();
        write_failures(&c, Some(2), 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.contains("Export failures (5 total, showing up to 2 from offset 0)"),
            "got:\n{s}"
        );
        assert!(s.contains("email #1"));
        assert!(s.contains("email #2"));
        assert!(!s.contains("email #3"));
        assert!(s.contains("... (3 more; --offset 2 --limit 2 to continue)"));
    }
}
