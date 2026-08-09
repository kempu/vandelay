/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use rusqlite::Connection;

pub const SCHEMA_SQL: &str = include_str!("schema.sql");

pub fn open(path: &std::path::Path) -> Result<Connection, OpenError> {
    let conn = Connection::open(path)?;
    apply_pragmas(&conn)?;
    apply_schema(&conn)?;
    Ok(conn)
}

pub fn apply_schema(conn: &Connection) -> Result<(), OpenError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(SCHEMA_SQL)?;
    ensure_calendar_events_data_type(&tx)?;
    tx.commit()?;
    Ok(())
}

fn ensure_calendar_events_data_type(conn: &Connection) -> Result<(), OpenError> {
    let mut stmt = conn.prepare("PRAGMA table_info(calendar_events)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let has_column = rows.filter_map(|r| r.ok()).any(|name| name == "data_type");
    if !has_column {
        conn.execute(
            "ALTER TABLE calendar_events ADD COLUMN data_type TEXT NOT NULL DEFAULT 'Event'",
            [],
        )?;
    }
    Ok(())
}

/// How long to wait for a competing holder of the archive before giving up.
///
/// MailPortal patch (see `docs/vandelay-patches.md`): with `locking_mode = EXCLUSIVE` and
/// no busy timeout, the FIRST write this connection makes fails instantly with
/// `database is locked` if any other process holds the file — no wait, no retry. That
/// turned every momentary overlap between two invocations on one archive into a hard,
/// unrecoverable run failure. A bounded wait absorbs the ordinary case (a previous
/// invocation still winding down) without hiding a genuinely stuck holder: past this the
/// error still surfaces, exactly as before.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn apply_pragmas(conn: &Connection) -> Result<(), OpenError> {
    // BEFORE the others, deliberately: `journal_mode` and `locking_mode` are themselves
    // statements that can take the database lock, so a timeout set after them would not
    // cover the two pragmas most likely to hit contention.
    conn.busy_timeout(BUSY_TIMEOUT)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}
