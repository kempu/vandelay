/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use super::common::{
    create_batch, jid, retry_if_blob_missing, retry_update_if_blob_missing, sole_blob,
    target_query_get, update_one,
};
use super::{Maps, Net, Plan, Quarantine, Uploader};
use crate::error::Error;
use crate::jmap::request::get_objects;
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{
    CALENDAR_EVENT_SELECT, CONTACT_CARD_SELECT, TargetResolver, calendar_event_to_wire,
    contact_card_to_wire,
};
use crate::sync::prune::{TargetObj, candidates};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

fn target_uid(v: &Value) -> Option<String> {
    v.get("uid").and_then(Value::as_str).map(str::to_owned)
}

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    ty: ObjectType,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
    unrecorded: &mut Vec<String>,
) -> Result<Plan, Error> {
    let mut targets = target_query_get(net, ty, None).map_err(Error::from)?;
    if ty == ObjectType::CalendarEvent {
        merge_calendar_event_controls(net, &mut targets)?;
    }
    let mut by_uid: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, target) in targets.iter().enumerate() {
        if let (Some(uid), Some(_)) = (target_uid(target), jid(target)) {
            by_uid.entry(uid).or_default().push(index);
        }
    }

    let select = if ty == ObjectType::ContactCard {
        CONTACT_CARD_SELECT
    } else {
        CALENDAR_EVENT_SELECT
    };
    let rows: Vec<(i64, String)> = {
        let mut stmt = ctx
            .conn
            .prepare(select)
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |r| {
            if ty == ObjectType::ContactCard {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            } else {
                let data: String = r.get(4)?;
                let uid = serde_json::from_str::<Value>(&data)
                    .ok()
                    .and_then(|v| v.get("uid").and_then(Value::as_str).map(str::to_owned))
                    .unwrap_or_default();
                Ok((r.get::<_, i64>(0)?, uid))
            }
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
    };

    let mut matched_target_ids: HashSet<String> = HashSet::new();
    let mut uploader = Uploader::new(net, &ctx.conn);
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;
    for (local, uid) in &rows {
        let preferred_calendars = if ty == ObjectType::CalendarEvent {
            desired_calendar_ids(ctx, *local, maps).ok()
        } else {
            None
        };
        let target_index = by_uid.get(uid).and_then(|candidates| {
            choose_target(
                candidates,
                &targets,
                &matched_target_ids,
                preferred_calendars.as_ref(),
            )
        });
        if let Some(target) = target_index.and_then(|index| targets.get(index))
            && let Some(tid) = jid(target)
        {
            maps.insert(ty, *local, JmapId(tid.clone()));
            matched_target_ids.insert(tid.clone());
            if ty == ObjectType::ContactCard {
                counts.skipped += 1;
                // It is on the target now, so any quarantine row a previous run
                // left behind describes a failure that has since been resolved.
                quarantine.resolve(*local, logger);
                continue;
            }

            // A UID match establishes identity, not convergence. In particular,
            // a previous export may have attached the event to a duplicate
            // calendar. Rebuild against this run's Calendar id map and update
            // every source-representable field that differs before calendar
            // pruning can remove the old container.
            let _ = uploader.take_touched();
            let wire = match build_wire(ctx, ty, *local, maps, &mut uploader) {
                Ok(wire) => wire,
                Err(e) => {
                    logger.warn(&format!(
                        "{} local {local} update skipped: {e}",
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
                    continue;
                }
            };
            let touched = uploader.take_touched();
            let update_patch = calendar_event_update_patch(target, &wire);
            if update_patch.is_empty() {
                counts.skipped += 1;
                quarantine.resolve(*local, logger);
                continue;
            }
            if net.dry_run {
                counts.updated += 1;
                continue;
            }

            let outcome = match update_one(net, ty, &tid, Value::Object(update_patch)) {
                Ok(outcome) => outcome,
                Err(e) => {
                    logger.warn(&format!(
                        "{} target {tid} update failed: {e}",
                        ty.jmap_name()
                    ));
                    counts.failed += 1;
                    quarantine.record_local_failure(
                        *local,
                        &format!("u{local}"),
                        sole_blob(&touched),
                        "updateFailed",
                        format!("target {tid}: {e}"),
                        logger,
                    );
                    continue;
                }
            };
            let (outcome, retry) = match retry_update_if_blob_missing(
                net,
                ty,
                &tid,
                &mut uploader,
                &touched,
                outcome,
                |up| {
                    let wire = build_wire(ctx, ty, *local, maps, up)?;
                    Ok(Value::Object(calendar_event_update_patch(target, &wire)))
                },
            ) {
                Ok(result) => result,
                Err(e) => {
                    logger.warn(&format!(
                        "{} target {tid} update retry failed: {e}",
                        ty.jmap_name()
                    ));
                    counts.failed += 1;
                    quarantine.record_local_failure(
                        *local,
                        &format!("u{local}"),
                        sole_blob(&touched),
                        "updateRetryFailed",
                        format!("target {tid}: {e}"),
                        logger,
                    );
                    continue;
                }
            };
            if outcome.updated.iter().any(|id| id == &tid) {
                counts.updated += 1;
                quarantine.resolve(*local, logger);
            } else {
                counts.failed += 1;
                let err = outcome
                    .not_updated
                    .iter()
                    .find(|(id, _)| id == &tid)
                    .map(|(_, err)| err);
                quarantine.record_update_refusal(
                    net,
                    *local,
                    &tid,
                    sole_blob(&touched),
                    err,
                    retry.as_ref(),
                    logger,
                );
            }
            continue;
        }
        let cid = format!("c{local}");
        let _ = uploader.take_touched();
        let wire = match build_wire(ctx, ty, *local, maps, &mut uploader) {
            Ok(w) => w,
            Err(e) => {
                logger.warn(&format!("{} local {local} skipped: {e}", ty.jmap_name()));
                quarantine.record_local_failure(
                    *local,
                    &cid,
                    None,
                    "buildFailed",
                    e.to_string(),
                    logger,
                );
                counts.failed += 1;
                continue;
            }
        };
        let touched = uploader.take_touched();
        let outcome = create_batch(net, ty, vec![(cid.clone(), wire)]).map_err(Error::from)?;
        let (outcome, retry) =
            match retry_if_blob_missing(net, ty, &cid, &mut uploader, &touched, outcome, |up| {
                build_wire(ctx, ty, *local, maps, up)
            }) {
                Ok(o) => o,
                Err(e) => {
                    logger.warn(&format!("{} local {local} skipped: {e}", ty.jmap_name()));
                    quarantine.record_local_failure(
                        *local,
                        &cid,
                        sole_blob(&touched),
                        "buildFailed",
                        format!("rebuilding after blobNotFound: {e}"),
                        logger,
                    );
                    counts.failed += 1;
                    continue;
                }
            };
        for (cid, v) in &outcome.created {
            if let Some(parsed) = cid.strip_prefix('c').and_then(|s| s.parse::<i64>().ok())
                && let Some(id) = jid(v)
            {
                maps.insert(ty, parsed, crate::jmap::wire::JmapId(id));
                counts.created += 1;
                quarantine.resolve(parsed, logger);
            }
        }
        for (cid, err) in &outcome.not_created {
            match cid.strip_prefix('c').and_then(|s| s.parse::<i64>().ok()) {
                Some(parsed) => quarantine.record_refusal(
                    net,
                    parsed,
                    cid,
                    sole_blob(&touched),
                    Some(err),
                    retry.as_ref(),
                    logger,
                ),
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
                matched: matched_target_ids.contains(&id),
                protected: false,
                may_delete: true,
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

const CALENDAR_EVENT_CONTROL_PROPERTIES: [&str; 4] = [
    "useDefaultAlerts",
    "mayInviteSelf",
    "mayInviteOthers",
    "hideAttendees",
];

fn merge_calendar_event_controls(net: &Net, targets: &mut [Value]) -> Result<(), Error> {
    let ids: Vec<JmapId> = targets.iter().filter_map(jid).map(JmapId).collect();
    if ids.is_empty() {
        return Ok(());
    }
    let controls = get_objects::<Value>(
        &net.client,
        &net.api,
        &net.account,
        ObjectType::CalendarEvent.jmap_name(),
        &ids,
        Some(&CALENDAR_EVENT_CONTROL_PROPERTIES),
        &net.limits,
    )
    .map_err(Error::from)?;
    let controls_by_id: HashMap<String, Value> = controls
        .list
        .into_iter()
        .filter_map(|value| jid(&value).map(|id| (id, value)))
        .collect();
    for target in targets {
        let Some(control) = jid(target).and_then(|id| controls_by_id.get(&id)) else {
            continue;
        };
        let Some(target) = target.as_object_mut() else {
            continue;
        };
        for property in CALENDAR_EVENT_CONTROL_PROPERTIES {
            if let Some(value) = control.get(property) {
                target.insert(property.to_owned(), value.clone());
            }
        }
    }
    Ok(())
}

fn desired_calendar_ids(ctx: &Context, local: i64, maps: &Maps) -> Result<HashSet<String>, Error> {
    let raw: String = ctx
        .conn
        .query_row(
            "SELECT calendar_ids FROM calendar_events WHERE id = ?1",
            rusqlite::params![local],
            |row| row.get(0),
        )
        .map_err(|e| Error::Partial(e.to_string()))?;
    let local_ids: Vec<i64> =
        serde_json::from_str(&raw).map_err(|e| Error::Partial(e.to_string()))?;
    local_ids
        .into_iter()
        .map(|calendar_id| {
            maps.target(ObjectType::Calendar, calendar_id)
                .map(|id| id.0)
                .ok_or_else(|| {
                    Error::Partial(format!(
                        "unresolved local Calendar id {calendar_id} while matching event {local}"
                    ))
                })
        })
        .collect()
}

fn target_calendar_ids(target: &Value) -> HashSet<String> {
    target
        .get("calendarIds")
        .and_then(Value::as_object)
        .map(|ids| {
            ids.iter()
                .filter(|(_, present)| present.as_bool() == Some(true))
                .map(|(id, _)| id.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn choose_target(
    candidates: &[usize],
    targets: &[Value],
    matched_target_ids: &HashSet<String>,
    preferred_calendars: Option<&HashSet<String>>,
) -> Option<usize> {
    let available = |index: &&usize| {
        targets
            .get(**index)
            .and_then(jid)
            .map(|id| !matched_target_ids.contains(&id))
            .unwrap_or(false)
    };
    if let Some(preferred) = preferred_calendars
        && let Some(index) = candidates
            .iter()
            .filter(available)
            .find(|index| target_calendar_ids(&targets[**index]) == *preferred)
    {
        return Some(*index);
    }
    candidates.iter().filter(available).copied().next()
}

fn calendar_event_update_patch(target: &Value, desired: &Value) -> Map<String, Value> {
    let Some(desired) = desired.as_object() else {
        return Map::new();
    };
    let mut patch: Map<String, Value> = desired
        .iter()
        .filter(|(property, value)| {
            calendar_event_property_is_writable(property) && target.get(*property) != Some(*value)
        })
        .map(|(property, value)| (property.clone(), value.clone()))
        .collect();
    let Some(target) = target.as_object() else {
        return patch;
    };
    for (property, value) in target {
        if desired.contains_key(property) || !calendar_event_property_is_writable(property) {
            continue;
        }
        let clear = match property.as_str() {
            "mayInviteSelf" | "mayInviteOthers" | "hideAttendees" => Value::Bool(false),
            _ => Value::Null,
        };
        if value != &clear {
            patch.insert(property.clone(), clear);
        }
    }
    patch
}

// Top-level CalendarEvent properties accepted by Stalwart v0.16's
// CalendarEvent/set update path. Keep this allow-list explicit: CalendarEvent/get
// also returns derived fields (`id`, `baseEventId`, `isOrigin`, `utcStart`,
// `utcEnd`) and immutable identity (`uid`), none of which may leak into a
// replica update merely because the source wire omitted them.
fn calendar_event_property_is_writable(property: &str) -> bool {
    matches!(
        property,
        "@type"
            | "alerts"
            | "calendarIds"
            | "categories"
            | "color"
            | "created"
            | "description"
            | "descriptionContentType"
            | "duration"
            | "endTimeZone"
            | "freeBusyStatus"
            | "hideAttendees"
            | "isDraft"
            | "keywords"
            | "links"
            | "locale"
            | "locations"
            | "mainLocationId"
            | "mayInviteOthers"
            | "mayInviteSelf"
            | "organizerCalendarAddress"
            | "participants"
            | "priority"
            | "privacy"
            | "prodId"
            | "recurrenceId"
            | "recurrenceIdTimeZone"
            | "recurrenceOverrides"
            | "recurrenceRule"
            | "relatedTo"
            | "replyTo"
            | "requestStatus"
            | "sentBy"
            | "sequence"
            | "showWithoutTime"
            | "start"
            | "status"
            | "timeZone"
            | "title"
            | "updated"
            | "useDefaultAlerts"
            | "virtualLocations"
    )
}

fn build_wire(
    ctx: &Context,
    ty: ObjectType,
    local: i64,
    maps: &Maps,
    up: &mut Uploader<'_>,
) -> Result<Value, Error> {
    if ty == ObjectType::ContactCard {
        let (uid, abids, data): (String, String, String) = ctx
            .conn
            .query_row(
                &format!("{CONTACT_CARD_SELECT} WHERE id = ?1"),
                rusqlite::params![local],
                |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map_err(|e| Error::Partial(e.to_string()))?;
        contact_card_to_wire(&uid, &abids, &data, maps, up).map_err(Error::from)
    } else {
        let (cal, dr, ud, data): (String, i64, i64, String) = ctx
            .conn
            .query_row(
                &format!("{CALENDAR_EVENT_SELECT} AND id = ?1"),
                rusqlite::params![local],
                |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .map_err(|e| Error::Partial(e.to_string()))?;
        calendar_event_to_wire(&cal, dr != 0, ud != 0, &data, maps, up).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn update_patch_clears_writable_optional_fields_but_never_server_fields() {
        let target = json!({
            "id": "EV",
            "uid": "same",
            "baseEventId": "BASE",
            "isOrigin": true,
            "utcStart": "2026-08-04T09:00:00Z",
            "utcEnd": "2026-08-04T10:00:00Z",
            "blobId": "SERVER-BLOB",
            "description": "stale",
            "alerts": {"old": {}},
            "mayInviteSelf": true,
            "title": "same"
        });
        let desired = json!({
            "uid": "same",
            "title": "same",
            "calendarIds": {"CAL": true},
            "isDraft": false,
            "useDefaultAlerts": false
        });
        let patch = calendar_event_update_patch(&target, &desired);
        assert_eq!(patch.get("description"), Some(&Value::Null));
        assert_eq!(patch.get("alerts"), Some(&Value::Null));
        assert_eq!(patch.get("mayInviteSelf"), Some(&Value::Bool(false)));
        assert_eq!(patch["calendarIds"], json!({"CAL": true}));
        for server_property in [
            "id",
            "uid",
            "baseEventId",
            "isOrigin",
            "utcStart",
            "utcEnd",
            "blobId",
        ] {
            assert!(
                !patch.contains_key(server_property),
                "{server_property} must never enter CalendarEvent/set"
            );
        }
    }

    #[test]
    fn duplicate_uid_targets_are_consumed_one_to_one_with_calendar_preference() {
        let targets = vec![
            json!({"id":"A","uid":"same","calendarIds":{"CAL-A":true}}),
            json!({"id":"B","uid":"same","calendarIds":{"CAL-B":true}}),
        ];
        let candidates = vec![0, 1];
        let mut matched = HashSet::new();
        let prefer_b = HashSet::from(["CAL-B".to_owned()]);
        let b = choose_target(&candidates, &targets, &matched, Some(&prefer_b)).unwrap();
        assert_eq!(jid(&targets[b]).as_deref(), Some("B"));
        matched.insert("B".to_owned());

        let prefer_a = HashSet::from(["CAL-A".to_owned()]);
        let a = choose_target(&candidates, &targets, &matched, Some(&prefer_a)).unwrap();
        assert_eq!(jid(&targets[a]).as_deref(), Some("A"));
    }
}
