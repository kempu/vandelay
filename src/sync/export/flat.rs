/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use rusqlite::params;
use serde_json::{Value, json};

use super::common::{create_batch, jid, target_get_all};
use super::{Maps, Net, Plan, Quarantine};
use crate::error::Error;
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{
    ADDRESS_BOOK_SELECT, CALENDAR_SELECT, row_to_address_book, row_to_calendar,
};
use crate::sync::keys::fold_name;
use crate::sync::prune::{TargetObj, candidates};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    ty: ObjectType,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
    unrecorded: &mut Vec<String>,
) -> Result<Plan, Error> {
    let table = crate::sync::table_name(ty);
    let select = if ty == ObjectType::AddressBook {
        ADDRESS_BOOK_SELECT
    } else {
        CALENDAR_SELECT
    };

    let locals: Vec<(i64, String, bool)> = {
        let mut stmt = ctx
            .conn
            .prepare(&format!("SELECT id, name, is_default FROM {table}"))
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |r| {
            Ok((r.get(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)? != 0))
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
    };

    let targets = target_get_all(net, ty).map_err(Error::from)?;
    let mut tmatched = std::collections::HashSet::new();
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;

    let mut to_create: Vec<(i64, bool)> = Vec::new();
    for (local, name, is_default) in &locals {
        let hit = targets.iter().find(|t| {
            let tid = jid(t);
            t.get("name")
                .and_then(Value::as_str)
                .map(|n| fold_name(n) == fold_name(name))
                .unwrap_or(false)
                && tid.as_ref().map(|i| !tmatched.contains(i)).unwrap_or(false)
        });
        match hit.and_then(jid) {
            Some(tid) => {
                tmatched.insert(tid.clone());
                maps.insert(ty, *local, JmapId(tid));
                counts.skipped += 1;
                // It is on the target now, so any quarantine row a previous run
                // left behind describes a failure that has since been resolved.
                quarantine.resolve(*local, logger);
            }
            None => to_create.push((*local, *is_default)),
        }
    }

    let mut batch = Vec::new();
    for (local, _) in &to_create {
        // Not `?`: one collection that will not build is ONE item. Letting it
        // out of here would end the whole pass on it — every other collection in
        // the batch would go unattempted, and `run()` would charge the abort a
        // single `failed` unit with no row behind it, so a caller reconciling
        // `failed=N` against the quarantine would find neither the item that
        // broke nor the ones that were never tried.
        match build_wire(ctx, ty, select, *local) {
            Ok(wire) => batch.push((format!("c{local}"), wire)),
            Err(e) => {
                logger.warn(&format!("{} local {local} skipped: {e}", ty.jmap_name()));
                quarantine.record_local_failure(
                    *local,
                    &format!("c{local}"),
                    None,
                    "buildFailed",
                    e.to_string(),
                    logger,
                );
                counts.failed += 1;
            }
        }
    }
    if !batch.is_empty() {
        let outcome = create_batch(net, ty, batch).map_err(Error::from)?;
        for (cid, v) in &outcome.created {
            if let Some(local) = cid.strip_prefix('c').and_then(|s| s.parse::<i64>().ok())
                && let Some(id) = jid(v)
            {
                maps.insert(ty, local, JmapId(id.clone()));
                counts.created += 1;
                quarantine.resolve(local, logger);
                if to_create.iter().any(|(l, d)| *l == local && *d) {
                    let mut req = crate::jmap::request::Request::new();
                    req.call(
                        format!("{}/set", ty.jmap_name()),
                        json!({ "accountId": net.account, "onSuccessSetIsDefault": id }),
                        "d",
                    );
                    if let Err(e) = req.send(&net.client, &net.api) {
                        logger.warn(&format!("{} isDefault not set: {e}", ty.jmap_name()));
                    }
                }
            }
        }
        for (cid, err) in &outcome.not_created {
            match cid.strip_prefix('c').and_then(|s| s.parse::<i64>().ok()) {
                Some(local) => {
                    quarantine.record_refusal(net, local, cid, None, Some(err), None, logger)
                }
                // The target answered with a creation id we never sent, so there
                // is no archive object to key a quarantine row on. Report it as
                // it stands rather than filing it against a guessed-at id.
                None => logger.warn(&format!("{} {cid} not created: {err}", ty.jmap_name())),
            }
            counts.failed += 1;
        }
    }

    let objs: Vec<TargetObj> = targets
        .iter()
        .filter_map(|t| {
            let id = jid(t)?;
            Some(TargetObj {
                id: id.clone(),
                matched: tmatched.contains(&id),
                protected: t.get("isDefault").and_then(Value::as_bool).unwrap_or(false),
                may_delete: t
                    .get("myRights")
                    .and_then(|r| r.get("mayDelete"))
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                parent: None,
            })
        })
        .collect();
    // Not `?`: the pass above completed and its counters are sound, so a failure
    // to write the sidecar rows travels as its own signal rather than as an error
    // that `run()` would charge to `failed`. See `Quarantine::finish`.
    if let Some(e) = quarantine.finish() {
        unrecorded.push(e);
    }
    Ok(Plan {
        prune_candidates: candidates(&objs, false),
        active_sieve_target: None,
    })
}

/// `isDefault` is not a create property here: it is applied after the fact
/// through `onSuccessSetIsDefault`, so it is stripped before the object goes up.
fn build_wire(ctx: &Context, ty: ObjectType, select: &str, local: i64) -> Result<Value, Error> {
    let mut wire = ctx
        .conn
        .query_row(&format!("{select} WHERE id = ?1"), params![local], |row| {
            Ok(if ty == ObjectType::AddressBook {
                row_to_address_book(row).map(|w| serde_json::to_value(&w))
            } else {
                row_to_calendar(row).map(|w| serde_json::to_value(&w))
            })
        })
        .map_err(|e| Error::Partial(e.to_string()))?
        .map_err(Error::from)?
        .map_err(|e| Error::Partial(e.to_string()))?;
    if let Value::Object(m) = &mut wire {
        m.remove("isDefault");
    }
    Ok(wire)
}
