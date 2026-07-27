/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use serde_json::{Value, json};

use crate::jmap::error::JmapError;
use crate::jmap::http::HttpClient;
use crate::jmap::request::{Request, URN_BLOB, check_method_error};
use crate::jmap::session::Session;
use crate::jmap::wire::JmapId;

pub fn upload_bytes(
    client: &HttpClient,
    session: &Session,
    account_id: &str,
    content_type: &str,
    bytes: &[u8],
) -> Result<JmapId, JmapError> {
    let url = session.upload_url_for(account_id);
    let response = client.upload(&url, content_type, bytes)?;
    response
        .get("blobId")
        .and_then(Value::as_str)
        .map(|s| JmapId(s.to_owned()))
        .ok_or_else(|| JmapError::malformed("upload response has no blobId"))
}

pub fn download_bytes(
    client: &HttpClient,
    session: &Session,
    account_id: &str,
    blob_id: &str,
    type_hint: &str,
    name: &str,
) -> Result<Vec<u8>, JmapError> {
    let url = session.download_url_for(account_id, blob_id, type_hint, name);
    client.download(&url)
}

/// Outcome of asking the target whether a blobId it handed us is still backed
/// by bytes it can serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobLookup {
    Found {
        size: Option<u64>,
    },
    NotFound,
    /// The target does not expose `urn:ietf:params:jmap:blob` for this account,
    /// or advertises it without implementing `Blob/get`. Retrievability is
    /// genuinely unknown; callers must report that rather than assume either way.
    Unsupported,
}

/// Ask the target whether `blob_id` is retrievable, without pulling the bytes
/// back over the wire.
///
/// `Blob/get` with `properties: ["size"]` makes the SERVER read the blob out of
/// its blob store to answer, so a blobId whose backing object has vanished lands
/// in `notFound` — the same verdict `Email/import` reaches internally, but for
/// the cost of a few hundred bytes of JSON instead of a second full copy of the
/// message. That matters because this probe only ever runs on a path that has
/// already paid for two uploads of the same message.
///
/// The capability is checked up front rather than inferred from an error, so a
/// target that simply lacks the blob extension is reported as `Unsupported`
/// instead of being mistaken for a missing blob.
pub fn lookup_blob(
    client: &HttpClient,
    session: &Session,
    account_id: &str,
    blob_id: &str,
) -> Result<BlobLookup, JmapError> {
    if !session.supports(account_id, URN_BLOB) {
        return Ok(BlobLookup::Unsupported);
    }
    let mut req = Request::new();
    req.call(
        "Blob/get",
        json!({
            "accountId": account_id,
            "ids": [blob_id],
            "properties": ["size"],
        }),
        "b",
    );
    let resp = req.send(client, &session.api_url)?;
    let mr = resp.first()?;
    match check_method_error(mr) {
        Ok(()) => {}
        Err(JmapError::UnknownMethod) => return Ok(BlobLookup::Unsupported),
        Err(e) => return Err(e),
    }
    if let Some(found) = mr
        .args
        .get("list")
        .and_then(Value::as_array)
        .and_then(|list| entry_for(list, blob_id))
    {
        return Ok(BlobLookup::Found {
            size: found.get("size").and_then(Value::as_u64),
        });
    }
    if mr
        .args
        .get("notFound")
        .and_then(Value::as_array)
        .is_some_and(|nf| nf.iter().any(|v| v.as_str() == Some(blob_id)))
    {
        return Ok(BlobLookup::NotFound);
    }
    Err(JmapError::malformed(format!(
        "Blob/get listed {blob_id} in neither list nor notFound"
    )))
}

/// Pick the `Blob/get` result object that answers for `blob_id`.
///
/// RFC 8620 5.1 has the server echo `id` on every object it returns even when
/// `properties` does not ask for it, so the ordinary case is an exact match on
/// `id`. An implementation that instead takes `properties: ["size"]` literally
/// answers with the size and nothing else. Since the probe asks about exactly
/// ONE id, a single unlabelled entry can only be describing that blob, and
/// reading it is the honest verdict: the target has just demonstrated that it
/// can read those bytes out of its blob store. Refusing to read it would fall
/// through to the malformed-response error, which `classify_blob` records as
/// `store_unavailable` — "the target blob store is unhealthy" — about a target
/// that is serving this very blob's metadata.
///
/// Anything else is deliberately left unmatched: an entry labelled with some
/// other id, or several entries in reply to a one-id request, supports no
/// inference worth making. The caller's remaining branches then take over, and
/// they are careful never to turn an unreadable answer into the `orphaned_marker`
/// verdict, which is the one this probe must never reach in error.
fn entry_for<'a>(list: &'a [Value], blob_id: &str) -> Option<&'a Value> {
    if let Some(exact) = list
        .iter()
        .find(|v| v.get("id").and_then(Value::as_str) == Some(blob_id))
    {
        return Some(exact);
    }
    match list {
        [only] if only.is_object() && only.get("id").is_none() => Some(only),
        _ => None,
    }
}
