/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use rusqlite::params;
use serde_json::{Map, Value};

use super::common::{create_batch, jid, target_get_all, target_query_get};
use super::{Maps, Net, Plan, Quarantine};
use crate::error::Error;
use crate::jmap::request::{SetRequest, set_call};
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
            .prepare(&format!(
                "SELECT id, name, is_default FROM {table} ORDER BY id"
            ))
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |r| {
            Ok((r.get(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)? != 0))
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
    };

    let targets = if ty == ObjectType::Calendar {
        // Stalwart's default Calendar/get property set omits several mutable
        // preference fields. Fetch them explicitly or a stale target value can
        // look identical to an absent/default source value and escape clearing.
        target_query_get(net, ty, Some(&CALENDAR_TARGET_PROPERTIES)).map_err(Error::from)?
    } else {
        target_get_all(net, ty).map_err(Error::from)?
    };
    let mut tmatched = std::collections::HashSet::new();
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;

    // A default calendar is an account-level identity, not a name. Stalwart
    // creates one for every account, so matching an imported default by name
    // would create a second calendar whenever the two installations chose
    // different display names. Reserve the target default before ordinary
    // name matching so an earlier non-default row cannot consume it.
    let target_default = (ty == ObjectType::Calendar)
        .then(|| {
            targets.iter().find(|target| {
                target
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    && jid(target).is_some()
            })
        })
        .flatten();
    let target_default_id = target_default.and_then(jid);
    let source_default = if ty == ObjectType::Calendar {
        let (source_default, inferred) =
            source_default_calendar(&locals, target_default_id.is_some())?;
        if inferred && let Some(local) = source_default {
            logger.warn(&format!(
                "calendar archive has no declared default; local {local} is used because the target requires one"
            ));
        }
        source_default
    } else {
        None
    };

    let mut to_create: Vec<(i64, bool)> = Vec::new();
    let mut to_update: Vec<(i64, String, Value)> = Vec::new();
    let mut matched_default_to_set: Option<(i64, String)> = None;
    for (local, name, is_default) in &locals {
        let hit = if source_default == Some(*local) {
            target_default.filter(|target| {
                jid(target)
                    .as_ref()
                    .map(|id| !tmatched.contains(id))
                    .unwrap_or(false)
            })
        } else {
            None
        }
        .or_else(|| {
            targets.iter().find(|target| {
                let tid = jid(target);
                let is_reserved_default = source_default.is_some()
                    && target_default_id.as_ref() == tid.as_ref()
                    && source_default != Some(*local);
                !is_reserved_default
                    && target
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|target_name| fold_name(target_name) == fold_name(name))
                        .unwrap_or(false)
                    && tid
                        .as_ref()
                        .map(|id| !tmatched.contains(id))
                        .unwrap_or(false)
            })
        });
        match hit.and_then(|target| jid(target).map(|tid| (target, tid))) {
            Some((target, tid)) => {
                tmatched.insert(tid.clone());
                maps.insert(ty, *local, JmapId(tid));
                if ty == ObjectType::Calendar {
                    let must_set_default = source_default == Some(*local)
                        && target.get("isDefault").and_then(Value::as_bool) != Some(true);
                    if must_set_default {
                        matched_default_to_set = jid(target).map(|id| (*local, id));
                    }
                    match build_calendar_update_wire(ctx, select, *local) {
                        Ok(wire) if calendar_needs_update(target, &wire) => {
                            let target_id = jid(target).expect("matched target has an id");
                            to_update.push((*local, target_id, wire));
                        }
                        Ok(_) => {
                            if !must_set_default {
                                counts.skipped += 1;
                                // It is on the target with the desired metadata
                                // now, so an earlier failure row is resolved.
                                quarantine.resolve(*local, logger);
                            }
                        }
                        Err(e) => {
                            logger.warn(&format!(
                                "{} local {local} metadata skipped: {e}",
                                ty.jmap_name()
                            ));
                            quarantine.record_local_failure(
                                *local,
                                &format!("u{local}"),
                                None,
                                "buildFailed",
                                e.to_string(),
                                logger,
                            );
                            counts.failed += 1;
                        }
                    }
                } else {
                    counts.skipped += 1;
                    // It is on the target now, so any quarantine row a previous
                    // run left behind describes a failure that has since resolved.
                    quarantine.resolve(*local, logger);
                }
            }
            None => to_create.push((
                *local,
                if ty == ObjectType::Calendar {
                    source_default == Some(*local)
                } else {
                    *is_default
                },
            )),
        }
    }

    apply_calendar_updates(net, &to_update, counts, &mut quarantine, logger);
    if let Some((local, target_id)) = matched_default_to_set {
        let metadata_was_updated = to_update
            .iter()
            .any(|(update_local, _, _)| *update_local == local);
        match set_collection_default(net, ObjectType::Calendar, &target_id) {
            Ok(()) => {
                if !metadata_was_updated {
                    counts.updated += 1;
                    quarantine.resolve(local, logger);
                }
            }
            Err(e) => {
                logger.warn(&format!(
                    "Calendar target {target_id} not made default: {e}"
                ));
                counts.failed += 1;
                quarantine.record_local_failure(
                    local,
                    &format!("u{local}"),
                    None,
                    "defaultSetFailed",
                    format!("target {target_id}: {e}"),
                    logger,
                );
            }
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
                if to_create.iter().any(|(l, d)| *l == local && *d)
                    && let Err(e) = set_collection_default(net, ty, &id)
                {
                    logger.warn(&format!(
                        "{} target {id} not made default: {e}",
                        ty.jmap_name()
                    ));
                    counts.failed += 1;
                    quarantine.record_local_failure(
                        local,
                        cid,
                        None,
                        "defaultSetFailed",
                        format!("target {id}: {e}"),
                        logger,
                    );
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

/// Resolve the single target-required default calendar from archive metadata.
///
/// CalDAV, EWS and Takeout sources do not identify a default and therefore
/// archive every collection with `is_default = false`. Reusing the lowest local
/// id for Stalwart's mandatory native default preserves the source collection
/// count and names. More than one declared default is contradictory metadata;
/// choosing one would silently falsify an exact replica, so fail before making
/// any target changes.
fn source_default_calendar(
    locals: &[(i64, String, bool)],
    target_has_default: bool,
) -> Result<(Option<i64>, bool), Error> {
    let defaults: Vec<i64> = locals
        .iter()
        .filter_map(|(local, _, is_default)| is_default.then_some(*local))
        .collect();
    match defaults.as_slice() {
        [] if target_has_default => Ok((
            locals.iter().map(|(local, _, _)| *local).min(),
            !locals.is_empty(),
        )),
        [] => Ok((None, false)),
        [only] => Ok((Some(*only), false)),
        many => Err(Error::Partial(format!(
            "calendar archive declares {} defaults ({})",
            many.len(),
            many.iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
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

/// Build a complete Calendar/set update patch from an archive row.
///
/// The Calendar wire format omits `None` fields for creates. An update cannot
/// omit them because omission means "leave the target value alone": nullable
/// scalar fields therefore become explicit nulls, while Stalwart requires the
/// default-alert maps to be objects and uses an empty object to clear them.
fn build_calendar_update_wire(ctx: &Context, select: &str, local: i64) -> Result<Value, Error> {
    let mut wire = build_wire(ctx, ObjectType::Calendar, select, local)?;
    let map = wire
        .as_object_mut()
        .ok_or_else(|| Error::Partial("calendar update is not an object".to_owned()))?;
    for property in ["description", "color", "timeZone"] {
        map.entry(property.to_owned()).or_insert(Value::Null);
    }
    for property in ["defaultAlertsWithTime", "defaultAlertsWithoutTime"] {
        map.entry(property.to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    Ok(wire)
}

const CALENDAR_UPDATE_PROPERTIES: [&str; 10] = [
    "name",
    "description",
    "color",
    "sortOrder",
    "isSubscribed",
    "isVisible",
    "includeInAvailability",
    "defaultAlertsWithTime",
    "defaultAlertsWithoutTime",
    "timeZone",
    // Keep this list explicit: server-owned id/isDefault/rights/ACL fields must
    // never be copied back in an update.
];

const CALENDAR_TARGET_PROPERTIES: [&str; 12] = [
    "name",
    "description",
    "color",
    "sortOrder",
    "isSubscribed",
    "isVisible",
    "isDefault",
    "includeInAvailability",
    "defaultAlertsWithTime",
    "defaultAlertsWithoutTime",
    "timeZone",
    "myRights",
];

fn calendar_needs_update(target: &Value, desired: &Value) -> bool {
    CALENDAR_UPDATE_PROPERTIES
        .iter()
        .any(|property| !calendar_property_matches(target, desired, property))
}

fn calendar_property_matches(target: &Value, desired: &Value, property: &str) -> bool {
    let Some(wanted) = desired.get(property) else {
        return false;
    };
    target.get(property) == Some(wanted)
}

fn apply_calendar_updates(
    net: &Net,
    updates: &[(i64, String, Value)],
    counts: &mut TypeCounts,
    quarantine: &mut Quarantine<'_>,
    logger: &Logger,
) {
    if updates.is_empty() {
        return;
    }
    if net.dry_run {
        counts.updated += updates.len() as u64;
        return;
    }

    let update = Value::Object(
        updates
            .iter()
            .map(|(_, target_id, wire)| (target_id.clone(), wire.clone()))
            .collect(),
    );
    match set_call(
        &net.client,
        &net.api,
        &net.account,
        ObjectType::Calendar.jmap_name(),
        SetRequest {
            update: Some(update),
            ..Default::default()
        },
        &net.limits,
    ) {
        Ok(outcome) => {
            let updated: std::collections::HashSet<&str> =
                outcome.updated.iter().map(String::as_str).collect();
            for (local, target_id, _) in updates {
                if updated.contains(target_id.as_str()) {
                    counts.updated += 1;
                    quarantine.resolve(*local, logger);
                } else if let Some((_, err)) =
                    outcome.not_updated.iter().find(|(id, _)| id == target_id)
                {
                    counts.failed += 1;
                    quarantine.record_update_refusal(
                        net,
                        *local,
                        target_id,
                        None,
                        Some(err),
                        None,
                        logger,
                    );
                } else {
                    counts.failed += 1;
                    quarantine
                        .record_update_refusal(net, *local, target_id, None, None, None, logger);
                }
            }
        }
        Err(e) => {
            for (local, target_id, _) in updates {
                counts.failed += 1;
                quarantine.record_local_failure(
                    *local,
                    &format!("u{local}"),
                    None,
                    "updateFailed",
                    format!("target {target_id}: {e}"),
                    logger,
                );
            }
        }
    }
}

fn set_collection_default(
    net: &Net,
    ty: ObjectType,
    target_id: &str,
) -> Result<(), crate::jmap::error::JmapError> {
    if net.dry_run {
        return Ok(());
    }
    let extra = [("onSuccessSetIsDefault", Value::String(target_id.to_owned()))];
    set_call(
        &net.client,
        &net.api,
        &net.account,
        ty.jmap_name(),
        SetRequest {
            extra_args: &extra,
            ..Default::default()
        },
        &net.limits,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_default_is_inferred_only_when_absent_and_multiple_are_rejected() {
        let absent = vec![
            (9, "Later".to_owned(), false),
            (3, "First".to_owned(), false),
        ];
        assert_eq!(
            source_default_calendar(&absent, true).unwrap(),
            (Some(3), true)
        );
        assert_eq!(
            source_default_calendar(&absent, false).unwrap(),
            (None, false)
        );

        let declared = vec![
            (9, "Later".to_owned(), false),
            (3, "First".to_owned(), true),
        ];
        assert_eq!(
            source_default_calendar(&declared, false).unwrap(),
            (Some(3), false)
        );

        let multiple = vec![(1, "One".to_owned(), true), (2, "Two".to_owned(), true)];
        let err =
            source_default_calendar(&multiple, true).expect_err("ambiguous default must fail");
        assert!(err.to_string().contains("declares 2 defaults (1, 2)"));
    }
}
