/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use super::common::{jid, target_query_get};
use super::{
    Maps, Net, Plan, Quarantine, Uploader, blob_identity, classify_blob, dedup_note,
    normalise_error_type, probe_note,
};
use crate::db::export_failures::{self, FailedItem, PROBE_NOT_PROBED};
use crate::error::Error;
use crate::jmap::error::JmapError;
use crate::jmap::request::{Request, check_method_error, get_objects};
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{EMAIL_SELECT, EmailRow, TargetResolver, row_to_email};
use crate::sync::keys::{EmailIndex, EmailKey, email_index, email_keys, index_from_json};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

fn server_index(v: &Value) -> EmailIndex {
    let arr = |k: &str| {
        v.get(k)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.get("email").and_then(Value::as_str).map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let mids: Vec<String> = v
        .get("messageId")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    email_index(
        &mids,
        &arr("from"),
        v.get("subject").and_then(Value::as_str).unwrap_or(""),
        v.get("sentAt").and_then(Value::as_str).unwrap_or(""),
        &arr("to"),
    )
}

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
    unrecorded: &mut Vec<String>,
) -> Result<Plan, Error> {
    let ty = ObjectType::Email;

    let target_min = target_query_get(net, ty, Some(&["messageId"])).map_err(Error::from)?;
    let mut indices: Vec<EmailIndex> = target_min.iter().map(server_index).collect();

    let fallback_ids: Vec<JmapId> = target_min
        .iter()
        .zip(indices.iter())
        .filter(|(_, i)| i.mids.is_empty())
        .filter_map(|(v, _)| jid(v).map(JmapId))
        .collect();
    if !fallback_ids.is_empty() {
        let got = get_objects::<Value>(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            &fallback_ids,
            Some(&["messageId", "from", "subject", "sentAt", "to"]),
            &net.limits,
        )
        .map_err(Error::from)?;
        let by_id: HashMap<String, &Value> = got
            .list
            .iter()
            .filter_map(|v| jid(v).map(|i| (i, v)))
            .collect();
        for (v, slot) in target_min.iter().zip(indices.iter_mut()) {
            if let Some(full) = jid(v).and_then(|i| by_id.get(&i)) {
                *slot = server_index(full);
            }
        }
    }
    let target_keys: HashSet<EmailKey> = email_keys(&indices).into_iter().collect();

    let local: Vec<(i64, EmailRow)> = {
        let mut stmt = ctx
            .conn
            .prepare(EMAIL_SELECT)
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            Ok((id, row_to_email(row)))
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
        .into_iter()
        .map(|(id, r)| Ok((id, r.map_err(Error::from)?)))
        .collect::<Result<_, Error>>()?
    };

    let local_indices: Vec<EmailIndex> = local
        .iter()
        .map(|(_, r)| index_from_json(&r.message_match))
        .collect();
    let local_keys = email_keys(&local_indices);

    let mut uploader = Uploader::new(net, &ctx.conn);
    let mut quarantine = Quarantine::open(&ctx.conn, ty, net.dry_run)?;
    for (i, key) in local_keys.iter().enumerate() {
        let (local_id, row) = &local[i];
        if target_keys.contains(key) {
            counts.skipped += 1;
            // The message is on the target now, so any quarantine row a previous
            // run left behind describes a failure that has since been resolved.
            quarantine.resolve(*local_id, logger);
            continue;
        }
        export_one(
            net,
            &mut uploader,
            maps,
            *local_id,
            row,
            counts,
            logger,
            &mut quarantine,
        );
    }
    // Not `?`: the pass above completed and its counters are sound, so a failure
    // to write the sidecar rows travels as its own signal rather than as an
    // error that `run()` would charge to `failed`. See `Quarantine::finish`.
    if let Some(e) = quarantine.finish() {
        unrecorded.push(e);
    }

    Ok(Plan::default())
}

fn build_mailbox_ids(row: &EmailRow, maps: &Maps) -> Option<Map<String, Value>> {
    let mut mids = Map::new();
    for ml in &row.mailbox_locals {
        let t = maps.target(ObjectType::Mailbox, *ml)?;
        mids.insert(t.0, Value::Bool(true));
    }
    Some(mids)
}

fn build_keywords(row: &EmailRow) -> Map<String, Value> {
    let mut kw = Map::new();
    for k in &row.keywords {
        kw.insert(k.clone(), Value::Bool(true));
    }
    kw
}

fn blob_hint(uploader: &Uploader, row: &EmailRow) -> String {
    use std::fmt::Write;

    let idx = index_from_json(&row.message_match);
    let mut s = match idx.mids.first() {
        Some(mid) => format!("message-id <{mid}>"),
        None => "no message-id".to_owned(),
    };
    match uploader.blob_len(row.blob_local_id) {
        Ok(Some(len)) => {
            let _ = write!(s, ", {}", crate::inspect::format_bytes(len));
        }
        Ok(None) => {}
        // The hint is decoration on a warning that is already being printed, so
        // it must not turn a failed archive read into a silently size-less line
        // that reads exactly like a message whose bytes are gone.
        Err(e) => {
            let _ = write!(s, ", size unreadable: {e}");
        }
    }
    s
}

fn size_note(e: &JmapError) -> &'static str {
    if matches!(
        e,
        JmapError::RequestTooLarge | JmapError::SingleObjectTooLarge(_)
    ) {
        "; exceeds the target server size limit, so this message is skipped and re-running will not migrate it"
    } else {
        ""
    }
}

fn import_item(
    blob: String,
    mids: Map<String, Value>,
    kw: Map<String, Value>,
    received_at: &str,
) -> Value {
    json!({
        "blobId": blob,
        "mailboxIds": Value::Object(mids),
        "keywords": Value::Object(kw),
        "receivedAt": received_at,
    })
}

/// Build the structured counterpart of the human-readable warning.
///
/// The warnings on stderr are for an operator watching the run; this row is for
/// whatever automation drives it, which previously could only read the aggregate
/// `failed=N` and had no way to say WHICH messages were refused. The full blake3
/// of the message bytes goes in deliberately: on a content-addressed target it
/// is the key the server itself de-duplicates on, so it is what correlates a
/// quarantined item with the marker sitting in the target's metadata store.
fn failed_item(
    uploader: &Uploader,
    cid: &str,
    local_id: i64,
    row: &EmailRow,
    error_type: &str,
    error_detail: String,
    logger: &Logger,
) -> FailedItem {
    let idx = index_from_json(&row.message_match);
    let blob = blob_identity(
        uploader.blob_len(row.blob_local_id),
        uploader.blob_hash(row.blob_local_id),
        ObjectType::Email,
        row.blob_local_id,
        logger,
    );
    let error_detail = blob.annotate(error_detail);
    FailedItem {
        type_name: ObjectType::Email.token().to_owned(),
        local_id,
        client_id: cid.to_owned(),
        message_id: idx.mids.first().cloned(),
        size_bytes: blob.size_bytes,
        blob_local_id: Some(row.blob_local_id),
        blob_hash: blob.hash,
        target_blob_id: None,
        error_type: normalise_error_type(error_type),
        error_detail,
        blob_probe: PROBE_NOT_PROBED.to_owned(),
        failed_at: export_failures::stamp_now(),
    }
}

#[allow(clippy::too_many_arguments)]
fn export_one(
    net: &Net,
    uploader: &mut Uploader,
    maps: &Maps,
    local_id: i64,
    row: &EmailRow,
    counts: &mut TypeCounts,
    logger: &Logger,
    quarantine: &mut Quarantine,
) {
    let cid = format!("e{local_id}");
    let mids = match build_mailbox_ids(row, maps) {
        Some(m) => m,
        None => {
            logger.warn(&format!(
                "Email/import {cid} ({}) skipped: mailbox not on target",
                blob_hint(uploader, row)
            ));
            quarantine.record(
                failed_item(
                    uploader,
                    &cid,
                    local_id,
                    row,
                    "mailboxNotOnTarget",
                    "the mailbox holding this message was not created on the target".to_owned(),
                    logger,
                ),
                logger,
            );
            counts.failed += 1;
            return;
        }
    };
    let blob = match uploader.upload_with(row.blob_local_id, "message/rfc822") {
        Ok(b) => b.0,
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob upload failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            quarantine.record(
                failed_item(
                    uploader,
                    &cid,
                    local_id,
                    row,
                    "blobUploadFailed",
                    format!("{e}{}", size_note(&e)),
                    logger,
                ),
                logger,
            );
            counts.failed += 1;
            return;
        }
    };
    if net.dry_run {
        counts.created += 1;
        return;
    }
    let item = import_item(blob.clone(), mids, build_keywords(row), &row.received_at);
    match send_single_import(net, &cid, item) {
        Ok(SingleImport::Created) => {
            counts.created += 1;
            quarantine.resolve(local_id, logger);
        }
        Ok(SingleImport::Skipped) => {
            counts.skipped += 1;
            quarantine.resolve(local_id, logger);
        }
        Ok(SingleImport::NotCreated { error_type, .. }) if error_type == "blobNotFound" => {
            retry_after_reupload(
                net, uploader, maps, &cid, local_id, row, &blob, counts, logger, quarantine,
            );
        }
        Ok(SingleImport::NotCreated { error_type, detail }) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) failed: {detail}",
                blob_hint(uploader, row)
            ));
            let mut item = failed_item(uploader, &cid, local_id, row, &error_type, detail, logger);
            item.target_blob_id = Some(blob);
            quarantine.record(item, logger);
            counts.failed += 1;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) send failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            let mut item = failed_item(
                uploader,
                &cid,
                local_id,
                row,
                "sendFailed",
                format!("{e}{}", size_note(&e)),
                logger,
            );
            item.target_blob_id = Some(blob);
            quarantine.record(item, logger);
            counts.failed += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn retry_after_reupload(
    net: &Net,
    uploader: &mut Uploader,
    maps: &Maps,
    cid: &str,
    local_id: i64,
    row: &EmailRow,
    first_blob: &str,
    counts: &mut TypeCounts,
    logger: &Logger,
    quarantine: &mut Quarantine,
) {
    uploader.invalidate(row.blob_local_id);
    let blob = match uploader.upload_with(row.blob_local_id, "message/rfc822") {
        Ok(b) => b.0,
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob re-upload failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            let mut item = failed_item(
                uploader,
                cid,
                local_id,
                row,
                "blobUploadFailed",
                format!("re-upload after blobNotFound: {e}{}", size_note(&e)),
                logger,
            );
            item.target_blob_id = Some(first_blob.to_owned());
            quarantine.record(item, logger);
            counts.failed += 1;
            return;
        }
    };
    let mids = match build_mailbox_ids(row, maps) {
        Some(m) => m,
        None => {
            logger.warn(&format!(
                "Email/import {cid} ({}) skipped: mailbox not on target",
                blob_hint(uploader, row)
            ));
            let mut item = failed_item(
                uploader,
                cid,
                local_id,
                row,
                "mailboxNotOnTarget",
                "the mailbox holding this message was not created on the target".to_owned(),
                logger,
            );
            item.target_blob_id = Some(blob);
            quarantine.record(item, logger);
            counts.failed += 1;
            return;
        }
    };
    let item = import_item(blob.clone(), mids, build_keywords(row), &row.received_at);
    match send_single_import(net, cid, item) {
        Ok(SingleImport::Created) => {
            counts.created += 1;
            quarantine.resolve(local_id, logger);
        }
        Ok(SingleImport::Skipped) => {
            counts.skipped += 1;
            quarantine.resolve(local_id, logger);
        }
        Ok(SingleImport::NotCreated { error_type, detail }) if error_type == "blobNotFound" => {
            let (probe, probe_detail) = classify_blob(net, &blob);
            let dedup = dedup_note(first_blob, &blob);
            logger.warn(&format!(
                "Email/import {cid} ({}) failed after blob re-upload: {detail}{dedup}{}",
                blob_hint(uploader, row),
                probe_note(&probe)
            ));
            let mut item = failed_item(
                uploader,
                cid,
                local_id,
                row,
                &error_type,
                match &probe_detail {
                    Some(text) => format!("{detail}; blob probe: {text}"),
                    None => detail,
                },
                logger,
            );
            item.target_blob_id = Some(blob);
            item.blob_probe = probe;
            quarantine.record(item, logger);
            counts.failed += 1;
        }
        Ok(SingleImport::NotCreated { error_type, detail }) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) failed after blob re-upload: {detail}",
                blob_hint(uploader, row)
            ));
            let mut item = failed_item(uploader, cid, local_id, row, &error_type, detail, logger);
            item.target_blob_id = Some(blob);
            quarantine.record(item, logger);
            counts.failed += 1;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) send failed after blob re-upload: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            let mut item = failed_item(
                uploader,
                cid,
                local_id,
                row,
                "sendFailed",
                format!("after blob re-upload: {e}{}", size_note(&e)),
                logger,
            );
            item.target_blob_id = Some(blob);
            quarantine.record(item, logger);
            counts.failed += 1;
        }
    }
}

enum SingleImport {
    Created,
    Skipped,
    NotCreated { error_type: String, detail: String },
}

fn send_single_import(net: &Net, cid: &str, item: Value) -> Result<SingleImport, JmapError> {
    let mut emails = Map::new();
    emails.insert(cid.to_owned(), item);
    let mut req = Request::new();
    req.call(
        "Email/import",
        json!({ "accountId": net.account, "emails": Value::Object(emails) }),
        "i",
    );
    req.fits(&net.limits)?;
    let resp = req.send(&net.client, &net.api)?;
    let mr = resp.first()?;
    check_method_error(mr)?;
    if let Some(err) = mr
        .args
        .get("notCreated")
        .and_then(Value::as_object)
        .and_then(|nc| nc.get(cid))
    {
        let error_type = err
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if error_type == "alreadyExists" {
            return Ok(SingleImport::Skipped);
        }
        return Ok(SingleImport::NotCreated {
            error_type,
            detail: err.to_string(),
        });
    }
    if mr
        .args
        .get("created")
        .and_then(Value::as_object)
        .is_some_and(|c| !c.is_empty())
    {
        return Ok(SingleImport::Created);
    }
    Ok(SingleImport::NotCreated {
        error_type: String::new(),
        detail: format!("Email/import returned neither created nor notCreated for {cid}"),
    })
}
