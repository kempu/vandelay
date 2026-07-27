/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;
use std::io::Write;

use rusqlite::Connection;
use serde_json::Value;

use super::{blob_summary, format_count};
use crate::error::Error;
use crate::types::ObjectType;

pub fn write_list(
    conn: &Connection,
    ty: ObjectType,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
) -> Result<(), Error> {
    let table = crate::sync::table_name(ty);
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
    let total = total as u64;
    write_header(out, ty, total, limit, offset)?;
    if total == 0 {
        writeln!(out, "(no {} in archive)", ty.token())?;
        return Ok(());
    }
    let names = Names::load(conn, ty)?;
    let mut written = 0u64;
    match ty {
        ObjectType::Identity => write_identities(conn, limit, offset, out, &mut written)?,
        ObjectType::Email => write_emails(conn, &names, limit, offset, out, &mut written)?,
        ObjectType::SieveScript => write_sieves(conn, limit, offset, out, &mut written)?,
        ObjectType::AddressBook => write_address_books(conn, limit, offset, out, &mut written)?,
        ObjectType::ContactCard => {
            write_contact_cards(conn, &names, limit, offset, out, &mut written)?
        }
        ObjectType::Calendar => write_calendars(conn, limit, offset, out, &mut written)?,
        ObjectType::CalendarEvent => {
            write_calendar_events(conn, &names, limit, offset, out, &mut written)?
        }
        ObjectType::ParticipantIdentity => {
            write_participant_identities(conn, limit, offset, out, &mut written)?
        }
        ObjectType::Mailbox | ObjectType::FileNode => unreachable!("tree types do not list"),
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

fn write_header(
    out: &mut impl Write,
    ty: ObjectType,
    total: u64,
    limit: Option<usize>,
    offset: usize,
) -> Result<(), Error> {
    match limit {
        None if offset == 0 => writeln!(out, "{} ({})", ty.jmap_name(), format_count(total))?,
        None => writeln!(
            out,
            "{} ({}, skipping first {})",
            ty.jmap_name(),
            format_count(total),
            format_count(offset as u64)
        )?,
        Some(lim) => writeln!(
            out,
            "{} ({} total, showing up to {} from offset {})",
            ty.jmap_name(),
            format_count(total),
            format_count(lim as u64),
            format_count(offset as u64)
        )?,
    }
    writeln!(out)?;
    Ok(())
}

pub(crate) struct Names {
    mailboxes: HashMap<i64, String>,
    address_books: HashMap<i64, String>,
    calendars: HashMap<i64, String>,
}

impl Names {
    pub(crate) fn load(conn: &Connection, ty: ObjectType) -> Result<Self, Error> {
        let mailboxes = if matches!(ty, ObjectType::Email) {
            load_name_map(conn, "SELECT id, name FROM mailboxes")?
        } else {
            HashMap::new()
        };
        let address_books = if matches!(ty, ObjectType::ContactCard) {
            load_name_map(conn, "SELECT id, name FROM address_books")?
        } else {
            HashMap::new()
        };
        let calendars = if matches!(ty, ObjectType::CalendarEvent) {
            load_name_map(conn, "SELECT id, name FROM calendars")?
        } else {
            HashMap::new()
        };
        Ok(Names {
            mailboxes,
            address_books,
            calendars,
        })
    }

    fn render(&self, kind: ObjectType, ids: &[i64]) -> String {
        let map = match kind {
            ObjectType::Mailbox => &self.mailboxes,
            ObjectType::AddressBook => &self.address_books,
            ObjectType::Calendar => &self.calendars,
            _ => return format!("{ids:?}"),
        };
        let parts: Vec<String> = ids
            .iter()
            .map(|id| match map.get(id) {
                Some(name) => format!("{name:?}"),
                None => format!("#{id} (missing)"),
            })
            .collect();
        format!("[{}]", parts.join(", "))
    }
}

fn load_name_map(conn: &Connection, sql: &str) -> Result<HashMap<i64, String>, Error> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
    let mut out = HashMap::new();
    for row in rows {
        let (id, name) = row?;
        out.insert(id, name);
    }
    Ok(out)
}

fn limit_clause(limit: Option<usize>, offset: usize) -> String {
    match limit {
        Some(n) => format!(" LIMIT {n} OFFSET {offset}"),
        None if offset > 0 => format!(" LIMIT -1 OFFSET {offset}"),
        None => String::new(),
    }
}

fn parse_local_ids(text: &str) -> Vec<i64> {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| {
            v.as_array()
                .map(|arr| arr.iter().filter_map(|x| x.as_i64()).collect::<Vec<_>>())
        })
        .unwrap_or_default()
}

fn json_pretty_or_raw(text: &str) -> String {
    match serde_json::from_str::<Value>(text) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|_| text.to_owned()),
        Err(_) => text.to_owned(),
    }
}

fn keywords_summary(text: &str) -> String {
    let ids = serde_json::from_str::<Vec<String>>(text).unwrap_or_default();
    if ids.is_empty() {
        "(none)".to_owned()
    } else {
        format!("[{}]", ids.join(", "))
    }
}

fn opt_or_null(s: Option<String>) -> String {
    s.unwrap_or_else(|| "(null)".to_owned())
}

fn bool_str(b: bool) -> String {
    (if b { "true" } else { "false" }).to_owned()
}

pub(super) fn write_record(
    out: &mut impl Write,
    title: &str,
    fields: &[(&str, String)],
) -> Result<(), Error> {
    writeln!(out, "{title}")?;
    let max_label = fields.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    let pad = " ".repeat(max_label + 4);
    for (label, value) in fields {
        for (i, line) in value.split('\n').enumerate() {
            if i == 0 {
                writeln!(out, "  {label:<max_label$}  {line}")?;
            } else {
                writeln!(out, "{pad}{line}")?;
            }
        }
    }
    writeln!(out)?;
    Ok(())
}

fn write_identities(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, name, email, reply_to, bcc, text_signature, html_signature
         FROM identities ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let fields = vec![
            ("name", row.get::<_, String>(1)?),
            ("email", row.get::<_, String>(2)?),
            ("reply_to", opt_or_null(row.get(3)?)),
            ("bcc", opt_or_null(row.get(4)?)),
            ("text_signature", row.get::<_, String>(5)?),
            ("html_signature", row.get::<_, String>(6)?),
        ];
        write_record(out, &format!("identity #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_emails(
    conn: &Connection,
    names: &Names,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, blob_id, received_at, mailbox_ids, keywords, message_match
         FROM emails ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let blob_id: i64 = row.get(1)?;
        let mailbox_ids: String = row.get(3)?;
        let keywords: String = row.get(4)?;
        let message_match: String = row.get(5)?;
        let locals = parse_local_ids(&mailbox_ids);
        let fields = vec![
            ("blob", blob_summary(conn, blob_id)?),
            ("received_at", row.get::<_, String>(2)?),
            ("mailboxes", names.render(ObjectType::Mailbox, &locals)),
            ("keywords", keywords_summary(&keywords)),
            ("message_match", json_pretty_or_raw(&message_match)),
        ];
        write_record(out, &format!("email #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_sieves(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, name, is_active, blob_id FROM sieve_scripts ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let name: Option<String> = row.get(1)?;
        let active: i64 = row.get(2)?;
        let blob_id: i64 = row.get(3)?;
        let fields = vec![
            ("name", opt_or_null(name)),
            ("is_active", bool_str(active != 0)),
            ("blob", blob_summary(conn, blob_id)?),
        ];
        write_record(out, &format!("sievescript #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_address_books(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, name, description, sort_order, is_default, is_subscribed
         FROM address_books ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let fields = vec![
            ("name", row.get::<_, String>(1)?),
            ("description", opt_or_null(row.get(2)?)),
            ("sort_order", row.get::<_, i64>(3)?.to_string()),
            ("is_default", bool_str(row.get::<_, i64>(4)? != 0)),
            ("is_subscribed", bool_str(row.get::<_, i64>(5)? != 0)),
        ];
        write_record(out, &format!("addressbook #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_contact_cards(
    conn: &Connection,
    names: &Names,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, uid, address_book_ids, data FROM contact_cards ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let uid: String = row.get(1)?;
        let ab_ids: String = row.get(2)?;
        let data: String = row.get(3)?;
        let locals = parse_local_ids(&ab_ids);
        let fields = vec![
            ("uid", uid),
            (
                "address_books",
                names.render(ObjectType::AddressBook, &locals),
            ),
            ("data", redact_blobs_in_json(&data)),
        ];
        write_record(out, &format!("contactcard #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_calendars(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, name, description, color, sort_order, is_subscribed, is_visible, is_default,
                include_in_availability, default_alerts_with_time, default_alerts_without_time,
                time_zone
         FROM calendars ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let fields = vec![
            ("name", row.get::<_, String>(1)?),
            ("description", opt_or_null(row.get(2)?)),
            ("color", opt_or_null(row.get(3)?)),
            ("sort_order", row.get::<_, i64>(4)?.to_string()),
            ("is_subscribed", bool_str(row.get::<_, i64>(5)? != 0)),
            ("is_visible", bool_str(row.get::<_, i64>(6)? != 0)),
            ("is_default", bool_str(row.get::<_, i64>(7)? != 0)),
            ("include_in_availability", row.get::<_, String>(8)?),
            ("default_alerts_with_time", opt_or_null(row.get(9)?)),
            ("default_alerts_without_time", opt_or_null(row.get(10)?)),
            ("time_zone", opt_or_null(row.get(11)?)),
        ];
        write_record(out, &format!("calendar #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_calendar_events(
    conn: &Connection,
    names: &Names,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, calendar_ids, is_draft, use_default_alerts, data
         FROM calendar_events ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let cal_ids: String = row.get(1)?;
        let data: String = row.get(4)?;
        let locals = parse_local_ids(&cal_ids);
        let fields = vec![
            ("calendars", names.render(ObjectType::Calendar, &locals)),
            ("is_draft", bool_str(row.get::<_, i64>(2)? != 0)),
            ("use_default_alerts", bool_str(row.get::<_, i64>(3)? != 0)),
            ("data", redact_blobs_in_json(&data)),
        ];
        write_record(out, &format!("calendarevent #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn write_participant_identities(
    conn: &Connection,
    limit: Option<usize>,
    offset: usize,
    out: &mut impl Write,
    written: &mut u64,
) -> Result<(), Error> {
    let sql = format!(
        "SELECT id, name, calendar_address, is_default
         FROM participant_identities ORDER BY id{}",
        limit_clause(limit, offset)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let fields = vec![
            ("name", row.get::<_, String>(1)?),
            ("calendar_address", row.get::<_, String>(2)?),
            ("is_default", bool_str(row.get::<_, i64>(3)? != 0)),
        ];
        write_record(out, &format!("participantidentity #{id}"), &fields)?;
        *written += 1;
    }
    Ok(())
}

fn redact_blobs_in_json(text: &str) -> String {
    let mut value = match serde_json::from_str::<Value>(text) {
        Ok(v) => v,
        Err(_) => return text.to_owned(),
    };
    redact_blobs(&mut value);
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| text.to_owned())
}

fn redact_blobs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(bid) = map.get("@blob").and_then(|v| v.as_i64()) {
                map.clear();
                map.insert(
                    "@blob".to_owned(),
                    Value::String(format!("local #{bid} (omitted)")),
                );
                return;
            }
            for v in map.values_mut() {
                redact_blobs(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_blobs(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{blobs, init};
    use rusqlite::params;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init::apply_schema(&c).unwrap();
        c
    }

    fn add_mailbox(c: &Connection, name: &str) -> i64 {
        c.execute("INSERT INTO mailboxes (name) VALUES (?1)", params![name])
            .unwrap();
        c.last_insert_rowid()
    }

    #[test]
    fn empty_table_prints_none_message() {
        let c = mem();
        let mut buf = Vec::new();
        write_list(&c, ObjectType::Identity, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Identity (0)"));
        assert!(s.contains("(no identity in archive)"));
    }

    #[test]
    fn identity_record_uses_aligned_fields_and_redacts_nothing() {
        let c = mem();
        c.execute(
            "INSERT INTO identities (name, email, text_signature) VALUES ('Alice', 'a@x.test', 'best,A')",
            [],
        )
        .unwrap();
        let mut buf = Vec::new();
        write_list(&c, ObjectType::Identity, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("identity #1"));
        assert!(s.contains("  name            Alice"), "got:\n{s}");
        assert!(s.contains("  email           a@x.test"));
        assert!(s.contains("  bcc             (null)"));
    }

    #[test]
    fn email_resolves_mailbox_names_and_summarises_blob() {
        let c = mem();
        let inbox = add_mailbox(&c, "Inbox");
        let other = add_mailbox(&c, "Personal");
        let blob = blobs::intern_blob(&c, b"From: a@x.test\r\nSubject: hi\r\n\r\nhello").unwrap();
        c.execute(
            "INSERT INTO emails (blob_id, received_at, mailbox_ids, keywords)
             VALUES (?1, '2024-01-01T00:00:00Z', ?2, ?3)",
            params![
                blob,
                format!("[{inbox},{other}]"),
                "[\"$seen\",\"Important\"]"
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        write_list(&c, ObjectType::Email, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("email #1"));
        assert!(s.contains("blob          "));
        assert!(s.contains("blake3="));
        assert!(s.contains("mailboxes     "));
        assert!(s.contains("\"Inbox\""), "got:\n{s}");
        assert!(s.contains("\"Personal\""));
        assert!(s.contains("keywords      "));
        assert!(s.contains("[$seen, Important]"));
    }

    #[test]
    fn contact_card_redacts_blob_sentinel_in_data() {
        let c = mem();
        c.execute("INSERT INTO address_books (name) VALUES ('Personal')", [])
            .unwrap();
        c.execute(
            "INSERT INTO contact_cards (uid, address_book_ids, data) VALUES (?1, ?2, ?3)",
            params![
                "urn:uuid:1",
                "[1]",
                r#"{"name":{"full":"Alice"},"photos":{"p":{"@blob":17}}}"#
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        write_list(&c, ObjectType::ContactCard, None, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("contactcard #1"));
        assert!(s.contains("\"Personal\""));
        assert!(s.contains("local #17 (omitted)"));
        assert!(!s.contains("\"@blob\": 17"));
    }

    #[test]
    fn pagination_writes_continuation_hint_when_more_remain() {
        let c = mem();
        for i in 0..5 {
            c.execute(
                "INSERT INTO identities (name, email) VALUES (?1, ?2)",
                params![format!("u{i}"), format!("u{i}@x.test")],
            )
            .unwrap();
        }
        let mut buf = Vec::new();
        write_list(&c, ObjectType::Identity, Some(2), 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Identity (5 total, showing up to 2 from offset 0)"));
        assert!(s.contains("identity #1"));
        assert!(s.contains("identity #2"));
        assert!(!s.contains("identity #3"));
        assert!(s.contains("... (3 more; --offset 2 --limit 2 to continue)"));
    }

    #[test]
    fn pagination_at_end_does_not_print_continuation() {
        let c = mem();
        for i in 0..3 {
            c.execute(
                "INSERT INTO identities (name, email) VALUES (?1, ?2)",
                params![format!("u{i}"), format!("u{i}@x.test")],
            )
            .unwrap();
        }
        let mut buf = Vec::new();
        write_list(&c, ObjectType::Identity, Some(5), 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(!s.contains("more;"), "got:\n{s}");
    }
}
