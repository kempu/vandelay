/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;

use rusqlite::{Connection, params};
use serde_json::Value;

use crate::db::exchange_graph_ids;
use crate::error::Error;
use crate::exchange_graph::api;
use crate::exchange_graph::calendar_map::{graph_calendar_color_to_hex, windows_or_iana_to_iana};
use crate::exchange_graph::types::MailboxKind;
use crate::logging::LEVEL_PROGRESS;
use crate::sync::TypeCounts;

use super::coordinator::{CHUNK_SIZE, GraphCoordinator, enumerate_mail_folders};

#[derive(Debug, Clone)]
pub struct MailFolder {
    pub graph_id: String,
    pub parent_graph_id: Option<String>,
    pub display_name: String,
    pub is_hidden: bool,
    pub local_id: i64,
}

#[derive(Debug, Clone)]
pub struct CalendarFolder {
    pub graph_id: String,
    pub local_id: i64,
}

#[derive(Debug, Clone)]
pub struct ContactFolder {
    pub graph_id: String,
    pub local_id: i64,
    /// True for the synthetic default Contacts folder, whose contacts are listed via
    /// `{me_or_user}/contacts` rather than `/contactFolders/{id}/contacts` (Patch 2).
    pub is_default: bool,
}

/// Synthetic graph id for the DEFAULT Contacts folder (`{me_or_user}/contacts`), which the
/// `/contactFolders` collection Graph returns excludes. Stable so re-runs update rather
/// than duplicate the address book, and never collides with a real (long, base64url) Graph
/// folder id (Patch 2).
pub const DEFAULT_CONTACTS_GRAPH_ID: &str = "mp-default-contacts";

pub fn reconcile_mail(
    conn: &mut Connection,
    ctx: &GraphCoordinator<'_>,
    mailbox_kind: MailboxKind,
    counts: &mut TypeCounts,
) -> Result<Vec<MailFolder>, Error> {
    let server = enumerate_mail_folders(ctx.client, ctx.endpoints, mailbox_kind, ctx.top)
        .map_err(Error::from)?;
    if ctx.logger.enabled(LEVEL_PROGRESS) {
        eprintln!("graph mailFolders enumerated: {}", server.len());
    }
    let well_known = match mailbox_kind {
        MailboxKind::Primary => resolve_well_known_roles(ctx),
        MailboxKind::Archive => HashMap::new(),
    };
    let local: HashMap<String, i64> =
        exchange_graph_ids::ids_of_type(conn, ctx.source_id, exchange_graph_ids::MAILBOX)?;

    let entries = order_by_parent(server);
    let mut by_id: HashMap<String, i64> = HashMap::new();
    let mut out: Vec<MailFolder> = Vec::new();
    let mut server_ids: Vec<String> = Vec::new();

    for chunk in entries.chunks(CHUNK_SIZE) {
        let tx = conn.unchecked_transaction()?;
        for value in chunk {
            let Some(graph_id) = value.get("id").and_then(Value::as_str) else {
                continue;
            };
            let parent_graph_id = value
                .get("parentFolderId")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let display_name = value
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or("Unnamed")
                .to_owned();
            let is_hidden = value
                .get("isHidden")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let role = well_known.get(graph_id).copied();
            let parent_local_id = parent_graph_id
                .as_deref()
                .and_then(|p| by_id.get(p))
                .copied();
            let existing = local.get(graph_id).copied();
            server_ids.push(graph_id.to_owned());

            let local_id = if let Some(id) = existing {
                let role = crate::db::roles::unique_role(&tx, role, Some(id))?;
                tx.execute(
                    "UPDATE mailboxes SET name = ?1, parent_id = ?2, role = ?3, is_subscribed = ?4
                     WHERE id = ?5",
                    params![display_name, parent_local_id, role, !is_hidden as i64, id,],
                )?;
                counts.fetched += 1;
                id
            } else {
                let role = crate::db::roles::unique_role(&tx, role, None)?;
                tx.execute(
                    "INSERT INTO mailboxes (name, parent_id, role, sort_order, is_subscribed)
                     VALUES (?1, ?2, ?3, 0, ?4)",
                    params![display_name, parent_local_id, role, !is_hidden as i64],
                )?;
                let new_id = tx.last_insert_rowid();
                exchange_graph_ids::insert(
                    &tx,
                    ctx.source_id,
                    exchange_graph_ids::MAILBOX,
                    graph_id,
                    new_id,
                )?;
                counts.created += 1;
                new_id
            };
            by_id.insert(graph_id.to_owned(), local_id);
            out.push(MailFolder {
                graph_id: graph_id.to_owned(),
                parent_graph_id,
                display_name,
                is_hidden,
                local_id,
            });
        }
        tx.commit()?;
    }

    delete_vanished_mailboxes(
        conn,
        ctx.source_id,
        &local,
        &server_ids,
        counts,
        &ctx.logger,
    )?;

    Ok(out)
}

pub fn reconcile_calendars(
    conn: &mut Connection,
    ctx: &GraphCoordinator<'_>,
    counts: &mut TypeCounts,
) -> Result<Vec<CalendarFolder>, Error> {
    let server = api::collect_all_values(ctx.client, &ctx.endpoints.calendars(ctx.top), &[])
        .map_err(Error::from)?;
    let mailbox_tz = mailbox_timezone(ctx)?;
    let local: HashMap<String, i64> =
        exchange_graph_ids::ids_of_type(conn, ctx.source_id, exchange_graph_ids::CALENDAR)?;
    let mut out: Vec<CalendarFolder> = Vec::new();
    let mut server_ids: Vec<String> = Vec::new();

    for chunk in server.chunks(CHUNK_SIZE) {
        let tx = conn.unchecked_transaction()?;
        for value in chunk {
            let Some(graph_id) = value.get("id").and_then(Value::as_str) else {
                continue;
            };
            server_ids.push(graph_id.to_owned());
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("Calendar")
                .to_owned();
            let color = value
                .get("hexColor")
                .and_then(Value::as_str)
                // Graph returns an empty hexColor when no explicit RGB colour
                // was chosen; that is absence, not a valid JMAP colour value.
                .filter(|color| !color.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    value
                        .get("color")
                        .and_then(Value::as_str)
                        .and_then(graph_calendar_color_to_hex)
                        .map(str::to_owned)
                });
            let is_default = value
                .get("isDefaultCalendar")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let existing = local.get(graph_id).copied();
            let tz = mailbox_tz.clone();
            let local_id = if let Some(id) = existing {
                // Graph Calendar directly supplies only the overlapping JMAP
                // fields name, colour, and default identity. Time zone is the
                // mailbox preference. The remaining JMAP calendar preferences
                // have no Graph Calendar equivalent, so reset them to the same
                // deterministic defaults used on first import instead of
                // retaining stale values from an older archive/source.
                tx.execute(
                    "UPDATE calendars SET name = ?1, description = NULL, color = ?2,
                         sort_order = 0, is_subscribed = 1, is_visible = 1, is_default = ?3,
                         include_in_availability = 'all', default_alerts_with_time = NULL,
                         default_alerts_without_time = NULL, time_zone = ?4
                     WHERE id = ?5",
                    params![name, color, is_default as i64, tz, id],
                )?;
                counts.fetched += 1;
                id
            } else {
                tx.execute(
                    "INSERT INTO calendars (name, color, sort_order, is_subscribed, is_visible,
                                              is_default, include_in_availability, time_zone)
                     VALUES (?1, ?2, 0, 1, 1, ?3, 'all', ?4)",
                    params![name, color, is_default as i64, tz],
                )?;
                let new_id = tx.last_insert_rowid();
                exchange_graph_ids::insert(
                    &tx,
                    ctx.source_id,
                    exchange_graph_ids::CALENDAR,
                    graph_id,
                    new_id,
                )?;
                counts.created += 1;
                new_id
            };
            out.push(CalendarFolder {
                graph_id: graph_id.to_owned(),
                local_id,
            });
        }
        tx.commit()?;
    }

    delete_vanished_flat(
        conn,
        ctx.source_id,
        exchange_graph_ids::CALENDAR,
        "calendars",
        &local,
        &server_ids,
        counts,
    )?;
    Ok(out)
}

pub fn reconcile_address_books(
    conn: &mut Connection,
    ctx: &GraphCoordinator<'_>,
    counts: &mut TypeCounts,
) -> Result<Vec<ContactFolder>, Error> {
    let mut server =
        api::collect_all_values(ctx.client, &ctx.endpoints.contact_folders(ctx.top), &[])
            .map_err(Error::from)?;
    let mut seen: std::collections::HashSet<String> = server
        .iter()
        .filter_map(|f| f.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect();
    let mut frontier: Vec<String> = seen.iter().cloned().collect();
    while let Some(parent) = frontier.pop() {
        let url = ctx.endpoints.contact_folder_children(&parent, ctx.top);
        let children = api::collect_all_values(ctx.client, &url, &[]).map_err(Error::from)?;
        for c in &children {
            if let Some(id) = c.get("id").and_then(Value::as_str)
                && seen.insert(id.to_owned())
            {
                frontier.push(id.to_owned());
            }
        }
        server.extend(children);
    }

    // Graph's /contactFolders collection excludes the default Contacts folder, so add it as
    // a synthetic address book (Patch 2). Its contacts are listed via {me_or_user}/contacts
    // in contacts::reconcile_all when book.is_default. Keyed by a stable synthetic id so the
    // id-map updates it on re-run instead of duplicating it.
    server.push(serde_json::json!({
        "id": DEFAULT_CONTACTS_GRAPH_ID,
        "displayName": "Contacts",
    }));

    let local: HashMap<String, i64> =
        exchange_graph_ids::ids_of_type(conn, ctx.source_id, exchange_graph_ids::ADDRESS_BOOK)?;
    let mut out: Vec<ContactFolder> = Vec::new();
    let mut server_ids: Vec<String> = Vec::new();
    for chunk in server.chunks(CHUNK_SIZE) {
        let tx = conn.unchecked_transaction()?;
        for value in chunk {
            let Some(graph_id) = value.get("id").and_then(Value::as_str) else {
                continue;
            };
            server_ids.push(graph_id.to_owned());
            let name = value
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or("Contacts")
                .to_owned();
            let existing = local.get(graph_id).copied();
            let local_id = if let Some(id) = existing {
                tx.execute(
                    "UPDATE address_books SET name = ?1 WHERE id = ?2",
                    params![name, id],
                )?;
                counts.fetched += 1;
                id
            } else {
                tx.execute(
                    "INSERT INTO address_books (name, sort_order, is_subscribed)
                     VALUES (?1, 0, 1)",
                    params![name],
                )?;
                let new_id = tx.last_insert_rowid();
                exchange_graph_ids::insert(
                    &tx,
                    ctx.source_id,
                    exchange_graph_ids::ADDRESS_BOOK,
                    graph_id,
                    new_id,
                )?;
                counts.created += 1;
                new_id
            };
            out.push(ContactFolder {
                graph_id: graph_id.to_owned(),
                local_id,
                is_default: graph_id == DEFAULT_CONTACTS_GRAPH_ID,
            });
        }
        tx.commit()?;
    }

    delete_vanished_flat(
        conn,
        ctx.source_id,
        exchange_graph_ids::ADDRESS_BOOK,
        "address_books",
        &local,
        &server_ids,
        counts,
    )?;
    Ok(out)
}

fn mailbox_timezone(ctx: &GraphCoordinator<'_>) -> Result<Option<String>, Error> {
    let url = ctx.endpoints.mailbox_settings_timezone();
    let value = ctx.client.get_json(&url).map_err(Error::from)?;
    mailbox_timezone_value(&value)
}

/// The mailbox time zone is a DEFAULT for the calendars imported from this
/// mailbox, and Graph does not guarantee one: a resource mailbox (room,
/// equipment) commonly reports `mailboxSettings` with no `timeZone` at all.
/// Absence is therefore ordinary and yields `None` — the calendars import with
/// no time-zone default, which is exactly what the column already models. It is
/// not a reason to abandon the mailbox: this used to return `Error::Partial`,
/// which aborted the whole run (mail included) with exit code 5 for every
/// resource mailbox.
///
/// A time zone that IS present but unparseable still fails: that is a real
/// mismatch between Graph and the IANA table, and silently dropping it would
/// import events against the wrong wall-clock.
fn mailbox_timezone_value(value: &Value) -> Result<Option<String>, Error> {
    let Some(tz) = value
        .get("timeZone")
        .and_then(Value::as_str)
        .filter(|tz| !tz.trim().is_empty())
    else {
        return Ok(None);
    };

    windows_or_iana_to_iana(tz)
        .ok_or_else(|| {
            Error::Partial(format!(
                "malformed: mailboxSettings returned unsupported timeZone {tz:?}"
            ))
        })
        .map(Some)
}

fn resolve_well_known_roles(ctx: &GraphCoordinator<'_>) -> HashMap<String, &'static str> {
    let mapping: &[(&str, &str)] = &[
        ("inbox", "inbox"),
        ("drafts", "drafts"),
        ("sentitems", "sent"),
        ("deleteditems", "trash"),
        ("junkemail", "junk"),
        ("archive", "archive"),
    ];
    let mut out = HashMap::new();
    for (short_name, role) in mapping {
        let url = ctx.endpoints.well_known_folder(short_name);
        match ctx.client.get_json(&url) {
            Ok(value) => {
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    out.insert(id.to_owned(), *role);
                }
            }
            Err(_) => continue,
        }
    }
    out
}

fn order_by_parent(server: Vec<Value>) -> Vec<Value> {
    let mut parents: HashMap<String, Option<String>> = HashMap::new();
    for v in &server {
        if let Some(id) = v.get("id").and_then(Value::as_str) {
            let parent = v
                .get("parentFolderId")
                .and_then(Value::as_str)
                .map(str::to_owned);
            parents.insert(id.to_owned(), parent);
        }
    }
    fn depth_of(
        id: &str,
        parents: &HashMap<String, Option<String>>,
        memo: &mut HashMap<String, usize>,
        seen: &mut std::collections::HashSet<String>,
    ) -> usize {
        if let Some(d) = memo.get(id) {
            return *d;
        }
        if !seen.insert(id.to_owned()) {
            return 0;
        }
        let parent = parents.get(id).and_then(|p| p.as_deref());
        let d = match parent {
            Some(p) if parents.contains_key(p) => 1 + depth_of(p, parents, memo, seen),
            _ => 0,
        };
        memo.insert(id.to_owned(), d);
        d
    }
    let mut memo: HashMap<String, usize> = HashMap::new();
    let mut entries: Vec<(usize, Value)> = server
        .into_iter()
        .map(|v| {
            let id = v.get("id").and_then(Value::as_str).unwrap_or("").to_owned();
            let mut seen = std::collections::HashSet::new();
            let d = depth_of(&id, &parents, &mut memo, &mut seen);
            (d, v)
        })
        .collect();
    entries.sort_by_key(|(d, _)| *d);
    entries.into_iter().map(|(_, v)| v).collect()
}

fn delete_vanished_flat(
    conn: &mut Connection,
    source_id: i64,
    type_name: &str,
    table: &str,
    local: &HashMap<String, i64>,
    server_ids: &[String],
    counts: &mut TypeCounts,
) -> Result<(), Error> {
    let server_set: std::collections::HashSet<&str> =
        server_ids.iter().map(String::as_str).collect();
    let vanished: Vec<(&String, &i64)> = local
        .iter()
        .filter(|(graph_id, _)| !server_set.contains(graph_id.as_str()))
        .collect();
    for chunk in vanished.chunks(CHUNK_SIZE) {
        let tx = conn.unchecked_transaction()?;
        for (graph_id, local_id) in chunk {
            let result = tx.execute(
                &format!("DELETE FROM {table} WHERE id = ?1"),
                params![local_id],
            );
            match result {
                Ok(_) => {
                    exchange_graph_ids::delete(&tx, source_id, type_name, graph_id)?;
                    counts.deleted += 1;
                }
                Err(_) => {
                    counts.failed += 1;
                }
            }
        }
        tx.commit()?;
    }
    Ok(())
}

fn delete_vanished_mailboxes(
    conn: &mut Connection,
    source_id: i64,
    local: &HashMap<String, i64>,
    server_ids: &[String],
    counts: &mut TypeCounts,
    logger: &crate::logging::Logger,
) -> Result<(), Error> {
    let server_set: std::collections::HashSet<&str> =
        server_ids.iter().map(String::as_str).collect();
    let mut vanished: Vec<(String, i64)> = local
        .iter()
        .filter(|(id, _)| !server_set.contains(id.as_str()))
        .map(|(id, lid)| (id.clone(), *lid))
        .collect();
    let depths = mailbox_depths(conn, &vanished)?;
    vanished.sort_by_key(|(_, local_id)| std::cmp::Reverse(*depths.get(local_id).unwrap_or(&0)));

    for (graph_id, local_id) in vanished {
        let tx = conn.unchecked_transaction()?;
        let result = tx.execute("DELETE FROM mailboxes WHERE id = ?1", params![local_id]);
        match result {
            Ok(_) => {
                exchange_graph_ids::delete(&tx, source_id, exchange_graph_ids::MAILBOX, &graph_id)?;
                tx.commit()?;
                counts.deleted += 1;
            }
            Err(e) => {
                let _ = tx.rollback();
                logger.warn(&format!(
                    "mailbox {graph_id} (local id {local_id}) could not be deleted (live children?): {e}"
                ));
                counts.failed += 1;
            }
        }
    }
    Ok(())
}

fn mailbox_depths(conn: &Connection, rows: &[(String, i64)]) -> Result<HashMap<i64, usize>, Error> {
    let mut parents: HashMap<i64, Option<i64>> = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT id, parent_id FROM mailboxes")?;
        let mut iter = stmt.query([])?;
        while let Some(row) = iter.next()? {
            let id: i64 = row.get(0)?;
            let parent: Option<i64> = row.get(1)?;
            parents.insert(id, parent);
        }
    }
    let mut depths: HashMap<i64, usize> = HashMap::new();
    for (_, local_id) in rows {
        depths.insert(*local_id, depth_of(*local_id, &parents));
    }
    Ok(depths)
}

fn depth_of(id: i64, parents: &HashMap<i64, Option<i64>>) -> usize {
    let mut depth = 0;
    let mut cursor = id;
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    while seen.insert(cursor) {
        match parents.get(&cursor).copied().flatten() {
            Some(p) => {
                depth += 1;
                cursor = p;
            }
            None => break,
        }
    }
    depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn order_by_parent_returns_parents_first() {
        let input = vec![
            json!({"id": "C", "parentFolderId": "B"}),
            json!({"id": "B", "parentFolderId": "A"}),
            json!({"id": "A"}),
        ];
        let out = order_by_parent(input);
        assert_eq!(out[0]["id"], "A");
        assert_eq!(out[1]["id"], "B");
        assert_eq!(out[2]["id"], "C");
    }

    #[test]
    fn order_by_parent_tolerates_missing_parent() {
        let input = vec![
            json!({"id": "Orphan", "parentFolderId": "Missing"}),
            json!({"id": "A"}),
        ];
        let out = order_by_parent(input);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn mailbox_timezone_absent_is_no_default_not_a_failure() {
        // A resource mailbox (room/equipment) reports mailboxSettings with no
        // timeZone. That must import the mailbox with no calendar time-zone
        // default, NOT abort the run — which is what failing here used to do.
        for value in [
            json!({}),
            json!({"timeZone": null}),
            json!({"timeZone": 7}),
            json!({"timeZone": ""}),
            json!({"timeZone": "   "}),
        ] {
            assert_eq!(
                mailbox_timezone_value(&value).expect("absent timeZone must not fail"),
                None,
                "value {value} should yield no default"
            );
        }
    }

    #[test]
    fn mailbox_timezone_maps_a_present_zone() {
        assert_eq!(
            mailbox_timezone_value(&json!({"timeZone": "FLE Standard Time"}))
                .expect("a known zone must map"),
            Some("Europe/Kiev".to_owned())
        );
    }

    #[test]
    fn mailbox_timezone_rejects_unknown_names() {
        // Present-but-unmappable stays fatal: importing events against the wrong
        // wall-clock is worse than stopping.
        let err = mailbox_timezone_value(&json!({"timeZone": "Made Up Zone"}))
            .expect_err("an unmapped timeZone must fail");
        assert!(
            err.to_string().contains("unsupported timeZone"),
            "got {err}"
        );
    }
}
