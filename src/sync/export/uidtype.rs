/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashSet;

use serde_json::Value;

use super::common::{create_batch, jid, retry_if_blob_missing, sole_blob, target_query_get};
use super::{Maps, Net, Plan, Quarantine, Uploader};
use crate::error::Error;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{
    CALENDAR_EVENT_SELECT, CONTACT_CARD_SELECT, calendar_event_to_wire, contact_card_to_wire,
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
    let targets = target_query_get(net, ty, None).map_err(Error::from)?;
    let mut by_uid: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for t in &targets {
        if let (Some(uid), Some(id)) = (target_uid(t), jid(t)) {
            by_uid.entry(uid).or_insert(id);
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

    let mut matched_uids: HashSet<String> = HashSet::new();
    let mut uploader = Uploader::new(net, &ctx.conn);
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;
    for (local, uid) in &rows {
        if let Some(tid) = by_uid.get(uid) {
            maps.insert(ty, *local, crate::jmap::wire::JmapId(tid.clone()));
            matched_uids.insert(uid.clone());
            counts.skipped += 1;
            // It is on the target now, so any quarantine row a previous run left
            // behind describes a failure that has since been resolved.
            quarantine.resolve(*local, logger);
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
            let uid = target_uid(t);
            Some(TargetObj {
                id: id.clone(),
                matched: uid.map(|u| matched_uids.contains(&u)).unwrap_or(false),
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
