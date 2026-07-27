/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use super::common::{create_batch, jid, retry_if_blob_missing, target_get_all};
use super::{Maps, Net, Plan, Quarantine, Uploader};
use crate::error::Error;
use crate::jmap::request::Request;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{SIEVE_SELECT, row_to_sieve_script};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    _maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
    unrecorded: &mut Vec<String>,
) -> Result<Plan, Error> {
    let ty = ObjectType::SieveScript;
    let targets = target_get_all(net, ty).map_err(Error::from)?;

    let mut target_by_name: HashMap<String, String> = HashMap::new();
    for t in &targets {
        let (Some(id), Some(name)) = (jid(t), t.get("name").and_then(Value::as_str)) else {
            continue;
        };
        target_by_name.insert(name.to_owned(), id);
    }

    let locals: Vec<(i64, Option<String>, bool, i64)> = {
        let mut stmt = ctx
            .conn
            .prepare(SIEVE_SELECT)
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |row| {
            let sr = row_to_sieve_script(row);
            Ok((row.get::<_, i64>(0)?, sr))
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
        .into_iter()
        .map(|(id, sr)| {
            let sr = sr.map_err(Error::from)?;
            Ok((id, sr.name, sr.is_active, sr.blob_local_id))
        })
        .collect::<Result<_, Error>>()?
    };

    let mut active_target: Option<String> = None;
    let mut deactivate = false;
    let mut uploader = Uploader::new(net, &ctx.conn);
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;

    for (local, name, is_active, blob_local) in &locals {
        let matched = name.as_ref().and_then(|n| target_by_name.get(n)).cloned();
        let target_id = if let Some(id) = matched {
            counts.skipped += 1;
            // The script is on the target now, so any quarantine row a previous
            // run left behind describes a failure that has since been resolved.
            quarantine.resolve(*local, logger);
            id
        } else {
            let cid = format!("c{local}");
            let build = |up: &mut Uploader<'_>| -> Result<Value, Error> {
                let blob_id = up
                    .upload_with(*blob_local, "application/sieve")
                    .map_err(Error::from)?;
                let mut obj = serde_json::Map::new();
                if let Some(n) = name {
                    obj.insert("name".to_owned(), Value::String(n.clone()));
                }
                obj.insert("blobId".to_owned(), Value::String(blob_id.0));
                Ok(Value::Object(obj))
            };
            let _ = uploader.take_touched();
            // Not `?`: this is ONE script giving up, and letting it out of the
            // loop would end the whole SieveScript pass on it — every script
            // after this one would go unattempted while `run()` charged the
            // abort a single `failed` unit with no row behind it. That is the
            // blind spot this table exists to close, and the worse half of it:
            // the count would under-report the loss as well as fail to name it.
            // `build` can only fail in `upload_with`, so the category is the
            // same `blobUploadFailed` the Email path records for a message whose
            // bytes would not go up.
            let wire = match build(&mut uploader) {
                Ok(w) => w,
                Err(e) => {
                    logger.warn(&format!("SieveScript local {local} skipped: {e}"));
                    quarantine.record_local_failure(
                        *local,
                        &cid,
                        Some(*blob_local),
                        "blobUploadFailed",
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
                match retry_if_blob_missing(net, ty, &cid, &mut uploader, &touched, outcome, build)
                {
                    Ok(o) => o,
                    // The retry re-uploads and re-sends, so the failure is either of
                    // those; `buildFailed` is the same category the other per-item
                    // surfaces record for the identically ambiguous case.
                    Err(e) => {
                        logger.warn(&format!("SieveScript local {local} skipped: {e}"));
                        quarantine.record_local_failure(
                            *local,
                            &cid,
                            Some(*blob_local),
                            "buildFailed",
                            format!("rebuilding after blobNotFound: {e}"),
                            logger,
                        );
                        counts.failed += 1;
                        continue;
                    }
                };
            match outcome.created.first().and_then(|(_, v)| jid(v)) {
                Some(id) => {
                    counts.created += 1;
                    quarantine.resolve(*local, logger);
                    if let Some(n) = name {
                        target_by_name.insert(n.clone(), id.clone());
                    }
                    id
                }
                None => {
                    // Only one creation id was sent, so anything else the target
                    // volunteered names no script of ours and cannot be filed
                    // against one; it is still reported rather than dropped.
                    for (other, err) in outcome.not_created.iter().filter(|(c, _)| *c != cid) {
                        logger.warn(&format!("SieveScript {other} not created: {err}"));
                    }
                    // A script is uploaded through the same content-addressed
                    // blob endpoint as a message, so `retry` carries the same
                    // evidence of a de-duplication marker shadowing bytes the
                    // target no longer has.
                    quarantine.record_refusal(
                        net,
                        *local,
                        &cid,
                        Some(*blob_local),
                        outcome
                            .not_created
                            .iter()
                            .find(|(c, _)| *c == cid)
                            .map(|(_, err)| err),
                        retry.as_ref(),
                        logger,
                    );
                    counts.failed += 1;
                    continue;
                }
            }
        };
        if *is_active {
            active_target = Some(target_id);
        }
    }

    if active_target.is_none() && locals.iter().all(|(_, _, a, _)| !*a) {
        deactivate = true;
    }

    if !net.dry_run {
        let mut req = Request::new();
        let args = if let Some(id) = &active_target {
            json!({ "accountId": net.account, "onSuccessActivateScript": id })
        } else if deactivate {
            json!({ "accountId": net.account, "onSuccessDeactivateScript": true })
        } else {
            json!({ "accountId": net.account })
        };
        req.call("SieveScript/set", args, "a");
        if let Err(e) = req.send(&net.client, &net.api) {
            logger.warn(&format!("SieveScript activation failed: {e}"));
        }
    }

    let local_names: HashSet<String> = locals.iter().filter_map(|(_, n, _, _)| n.clone()).collect();
    let mut prune_candidates: Vec<String> = target_by_name
        .iter()
        .filter(|(name, _)| !local_names.contains(*name))
        .map(|(_, id)| id.clone())
        .collect();
    prune_candidates.sort();

    // Not `?`: the pass above completed and its counters are sound, so a failure
    // to write the sidecar rows travels as its own signal rather than as an error
    // that `run()` would charge to `failed`. See `Quarantine::finish`.
    if let Some(e) = quarantine.finish() {
        unrecorded.push(e);
    }

    Ok(Plan {
        prune_candidates,
        active_sieve_target: active_target,
    })
}
