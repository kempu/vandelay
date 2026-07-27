/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;
use std::io::{IsTerminal, Write};

use rusqlite::Connection;
use serde_json::{Map, Value, json};

use crate::db;
use crate::db::export_failures::{
    PROBE_NOT_PROBED, PROBE_ORPHANED_MARKER, PROBE_RETRIEVABLE, PROBE_STORE_UNAVAILABLE,
    PROBE_UNSUPPORTED,
};
use crate::error::Error;
use crate::jmap::blobxfer::{self, BlobLookup};
use crate::jmap::connect::{self, Connected};
use crate::jmap::error::JmapError;
use crate::jmap::http::HttpClient;
use crate::jmap::request::{Request, SetRequest, get_all, get_objects, query_all_ids, set_call};
use crate::jmap::session::{Limits, Session};
use crate::jmap::wire::JmapId;
use crate::logging::{LEVEL_DEFAULT, Logger};
use crate::sync::import_jmap::mapping::{BlobUpload, TargetResolver};
use crate::sync::{CommonConfig, Context, ExportConfig, Summary, TypeCounts};
use crate::types::ObjectType;

const EXPORT_ORDER: [ObjectType; 10] = [
    ObjectType::Mailbox,
    ObjectType::AddressBook,
    ObjectType::Calendar,
    ObjectType::FileNode,
    ObjectType::Identity,
    ObjectType::SieveScript,
    ObjectType::ParticipantIdentity,
    ObjectType::Email,
    ObjectType::ContactCard,
    ObjectType::CalendarEvent,
];

type IdMap = HashMap<i64, JmapId>;

#[derive(Default)]
struct Maps {
    m: HashMap<ObjectType, IdMap>,
}

impl Maps {
    fn insert(&mut self, ty: ObjectType, local: i64, target: JmapId) {
        self.m.entry(ty).or_default().insert(local, target);
    }
}

impl TargetResolver for Maps {
    fn target(&self, ty: ObjectType, local_id: i64) -> Option<JmapId> {
        self.m.get(&ty)?.get(&local_id).cloned()
    }
}

struct Uploader<'a> {
    net: &'a Net,
    conn: &'a Connection,
    cache: HashMap<i64, JmapId>,
    touched: Vec<i64>,
}

impl<'a> Uploader<'a> {
    fn new(net: &'a Net, conn: &'a Connection) -> Uploader<'a> {
        Uploader {
            net,
            conn,
            cache: HashMap::new(),
            touched: Vec::new(),
        }
    }

    fn upload_with(&mut self, local_id: i64, content_type: &str) -> Result<JmapId, JmapError> {
        self.touched.push(local_id);
        if let Some(id) = self.cache.get(&local_id) {
            return Ok(id.clone());
        }
        let id = if self.net.dry_run {
            let _exists = db::blobs::blob_bytes(self.conn, local_id)?
                .ok_or_else(|| JmapError::malformed(format!("blob local id {local_id} missing")))?;
            JmapId(format!("dryrun-blob-{local_id}"))
        } else {
            let bytes = db::blobs::blob_bytes(self.conn, local_id)?
                .ok_or_else(|| JmapError::malformed(format!("blob local id {local_id} missing")))?;
            blobxfer::upload_bytes(
                &self.net.client,
                &self.net.session,
                &self.net.account,
                content_type,
                &bytes,
            )?
        };
        self.cache.insert(local_id, id.clone());
        Ok(id)
    }

    fn invalidate(&mut self, local_id: i64) {
        self.cache.remove(&local_id);
    }

    // A read that FAILS and a blob that is GONE are two different facts, and
    // these must not flatten them into one `None`. The quarantine stores a null
    // for both, `inspect --failures` reads a null hash as "the bytes are gone",
    // and the blake3 is the single field that correlates a quarantined item with
    // the de-duplication marker the target is still holding — so a transient
    // SQLITE_BUSY swallowed here would quietly destroy the one thing the row is
    // written to carry, and say something untrue about the archive on the way
    // out. The error goes to the caller, which records it as what it is.
    fn blob_len(&self, local_id: i64) -> Result<Option<u64>, rusqlite::Error> {
        db::blobs::blob_len(self.conn, local_id)
    }

    fn blob_hash(&self, local_id: i64) -> Result<Option<String>, rusqlite::Error> {
        db::blobs::blob_hash_hex(self.conn, local_id)
    }

    fn cached(&self, local_id: i64) -> Option<String> {
        self.cache.get(&local_id).map(|id| id.0.clone())
    }

    fn take_touched(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.touched)
    }
}

/// The blobIds a `blobNotFound` retry observed for an object backed by exactly
/// one blob.
///
/// The retry re-uploads the SAME bytes, so on a content-addressed target they
/// hash to the same key: a second blobId identical to the first is the target
/// telling us it de-duplicated rather than stored anything. That pair is the
/// evidence the quarantine needs, so it is carried out of the retry instead of
/// being reconstructed afterwards from a cache the rebuild has already churned.
///
/// An object assembled from several blobs is deliberately not described this
/// way: `notCreated` names no blob, so there is nothing to attribute the refusal
/// to and inventing an attribution would be worse than leaving the columns NULL.
struct BlobRetry {
    first: String,
    second: String,
}

/// Writer for the per-item failure table.
///
/// Export is otherwise a pure reader of the archive; this is the one place it
/// writes, which is why every mutation goes through `enabled`. A `--dry-run`
/// must stay side-effect free, so in that mode the quarantine is inert.
///
/// `known` holds the local ids that already carried a row when the run started.
/// It exists so `resolve()` can skip the DELETE for the overwhelming majority
/// of items that never failed — a re-run over a large archive matches hundreds
/// of thousands of messages, and issuing a write per match would turn a
/// read-only reconcile into that many WAL commits.
struct Quarantine<'a> {
    conn: &'a Connection,
    ty: ObjectType,
    enabled: bool,
    known: std::collections::HashSet<i64>,
    write_error: Option<String>,
}

impl<'a> Quarantine<'a> {
    fn open(conn: &'a Connection, ty: ObjectType, dry_run: bool) -> Result<Quarantine<'a>, Error> {
        let known = if dry_run {
            std::collections::HashSet::new()
        } else {
            db::export_failures::local_ids(conn, ty.token())?
                .into_iter()
                .collect()
        };
        Ok(Quarantine {
            conn,
            ty,
            enabled: !dry_run,
            known,
            write_error: None,
        })
    }

    fn record(&mut self, item: db::export_failures::FailedItem, logger: &Logger) {
        if !self.enabled {
            return;
        }
        let local_id = item.local_id;
        match db::export_failures::record(self.conn, &item) {
            Ok(()) => {
                self.known.insert(local_id);
            }
            Err(e) => self.note_write_error(e, logger),
        }
    }

    /// The columns every quarantine row shares, before the type-specific ones.
    ///
    /// Kept on the writer rather than free-standing so a reconciler cannot get
    /// the type token wrong: it is half the primary key, so a row filed under a
    /// token `resolve()` never looks up is a failure that can never clear.
    fn row(
        &self,
        local_id: i64,
        cid: &str,
        error_type: &str,
        error_detail: String,
    ) -> db::export_failures::FailedItem {
        db::export_failures::FailedItem {
            type_name: self.ty.token().to_owned(),
            local_id,
            client_id: cid.to_owned(),
            message_id: None,
            size_bytes: None,
            blob_local_id: None,
            blob_hash: None,
            target_blob_id: None,
            error_type: normalise_error_type(error_type),
            error_detail,
            blob_probe: PROBE_NOT_PROBED.to_owned(),
            failed_at: db::export_failures::stamp_now(),
        }
    }

    /// Fill in the blob columns of a row, or leave them null when the failure
    /// cannot be pinned on one blob.
    ///
    /// Shared by every writer so a row's blob identity means the same thing
    /// whichever pass recorded it, and in particular so a column that is absent
    /// because the archive read FAILED is never handed over looking like a blob
    /// that is genuinely gone — see `blob_identity`.
    fn with_blob(
        &self,
        mut item: db::export_failures::FailedItem,
        blob_local_id: Option<i64>,
        logger: &Logger,
    ) -> db::export_failures::FailedItem {
        let Some(b) = blob_local_id else {
            return item;
        };
        let blob = blob_identity(
            db::blobs::blob_len(self.conn, b),
            db::blobs::blob_hash_hex(self.conn, b),
            self.ty,
            b,
            logger,
        );
        item.error_detail = blob.annotate(item.error_detail);
        item.blob_local_id = Some(b);
        item.size_bytes = blob.size_bytes;
        item.blob_hash = blob.hash;
        item
    }

    /// Record an item the export gave up on before the target ever saw it.
    ///
    /// The caller has already reported it in the terms that suit its own pass, so
    /// this only writes the row. `error_type` names the category, which is what
    /// lets a reader tell a local give-up — a parent that was never created, a
    /// row that would not build — apart from something the server refused.
    ///
    /// `blob_local_id` is set when the pass gave up at a point where exactly one
    /// blob was in play, which is what lets the row carry the size and blake3
    /// that make the item findable; a give-up that cannot name a single blob —
    /// a parent that was never created, a row that would not read — passes
    /// `None` rather than pointing at whichever blob happened to be nearby.
    fn record_local_failure(
        &mut self,
        local_id: i64,
        cid: &str,
        blob_local_id: Option<i64>,
        error_type: &str,
        error_detail: String,
        logger: &Logger,
    ) {
        let item = self.row(local_id, cid, error_type, error_detail);
        let item = self.with_blob(item, blob_local_id, logger);
        self.record(item, logger);
    }

    /// Record and report an object the target itself refused to create.
    ///
    /// Every reconciler funnels its `notCreated` entries through here so that a
    /// refusal leaves the same structured trace whatever the object type. While
    /// only the Email path wrote rows, a run could print `SieveScript: created=0
    /// failed=1`, exit 5, and still leave `inspect --failures` reporting `Export
    /// failures (0)` — a caller reading the table as "what was refused" would
    /// have read that run as clean. Sieve scripts and file nodes go up through
    /// the very same content-addressed blob endpoint as messages, so the orphaned
    /// de-duplication marker this table exists for can shadow one of those just
    /// as easily.
    ///
    /// `retry` is set only when the refusal survived a `blobNotFound` re-upload,
    /// which is the only situation in which probing the target's blob store says
    /// anything; it is the same probe, on the same evidence, that the Email path
    /// runs after its own re-upload.
    #[allow(clippy::too_many_arguments)]
    fn record_refusal(
        &mut self,
        net: &Net,
        local_id: i64,
        cid: &str,
        blob_local_id: Option<i64>,
        err: Option<&Value>,
        retry: Option<&BlobRetry>,
        logger: &Logger,
    ) {
        let (error_type, detail) = match err {
            Some(e) => (
                e.get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                e.to_string(),
            ),
            // The target listed this creation id under neither `created` nor
            // `notCreated`. The object is still not on the target, so it is still
            // a refusal, and saying so beats charging a `failed` unit with no row
            // behind it — the exact blind spot the table was added to close.
            None => (
                String::new(),
                format!(
                    "{}/set returned neither created nor notCreated for {cid}",
                    self.ty.jmap_name()
                ),
            ),
        };
        let item = self.row(local_id, cid, &error_type, detail);
        let mut item = self.with_blob(item, blob_local_id, logger);
        if let Some(r) = retry {
            item.target_blob_id = Some(r.second.clone());
            if item.error_type == "blobNotFound" {
                let (probe, probe_detail) = classify_blob(net, &r.second);
                if let Some(text) = probe_detail {
                    item.error_detail = format!("{}; blob probe: {text}", item.error_detail);
                }
                item.blob_probe = probe;
            }
        }
        logger.warn(&format!(
            "{} {cid} not created: {}{}{}",
            self.ty.jmap_name(),
            item.error_detail,
            retry.map_or("", |r| dedup_note(&r.first, &r.second)),
            probe_note(&item.blob_probe)
        ));
        self.record(item, logger);
    }

    /// Sweep the rows `resolve()` can never reach, once per run.
    ///
    /// See `db::export_failures::reap_missing` for why a row can end up naming
    /// an archive object that is gone and why leaving it there is a permanent
    /// phantom failure plus a permanent blob leak. Every type is swept rather
    /// than just this run's work list: `has_rows` drops a type whose table has
    /// been emptied outright, and that is precisely the case where every row for
    /// it is stale, so a sweep hung off the per-type pass would never reach it.
    fn reap_stale(conn: &Connection, dry_run: bool, logger: &Logger) -> Result<(), Error> {
        if dry_run {
            return Ok(());
        }
        for ty in ObjectType::ALL {
            let n =
                db::export_failures::reap_missing(conn, ty.token(), crate::sync::table_name(ty))?;
            if n > 0 {
                logger.warn(&format!(
                    "quarantine: dropped {n} recorded {} failure(s) whose archive object no \
                     longer exists; a re-import renumbers local ids, so those rows could no \
                     longer name an item",
                    ty.jmap_name()
                ));
            }
        }
        Ok(())
    }

    fn resolve(&mut self, local_id: i64, logger: &Logger) {
        if !self.enabled || !self.known.remove(&local_id) {
            return;
        }
        if let Err(e) = db::export_failures::resolve(self.conn, self.ty.token(), local_id) {
            self.note_write_error(e, logger);
        }
    }

    // A quarantine write that fails is not a detail we may drop: the run would
    // otherwise report `failed=N` on stdout with nothing behind it, which is the
    // exact blind spot this table was added to close. Surface it immediately and
    // again as a run-level signal once the pass is over.
    fn note_write_error(&mut self, e: rusqlite::Error, logger: &Logger) {
        logger.error(&format!(
            "{}: recording the per-item failure in the archive failed: {e}",
            self.ty.jmap_name()
        ));
        if self.write_error.is_none() {
            self.write_error = Some(e.to_string());
        }
    }

    /// The bookkeeping failure this pass hit, if any, for the caller to hand to
    /// `Summary::unrecorded_failures`.
    ///
    /// Reported as a value rather than raised as an `Error` on purpose. A type
    /// that returns an error to `run()` is charged one `failed` unit — the
    /// sentinel for "this type aborted, its counters are incomplete" — but the
    /// pass this ends did NOT abort: every item was attempted and counted, and
    /// only the sidecar record of some of them is missing. Charging it there
    /// would inflate the very `failed=N` these rows exist to explain, and a
    /// consumer reconciling that number against the rows would find one unit
    /// too many with no item behind it.
    fn finish(self) -> Option<String> {
        let ty = self.ty.jmap_name();
        self.write_error
            .map(|e| format!("{ty} failures could not be recorded in the archive: {e}"))
    }
}

/// Failures raised locally never carry a target error type, and a target can
/// also answer with an empty one. Neither may end up as a blank column: the
/// reader has to be able to tell the categories apart.
fn normalise_error_type(error_type: &str) -> String {
    if error_type.is_empty() {
        "unknown".to_owned()
    } else {
        error_type.to_owned()
    }
}

struct BlobIdentity {
    size_bytes: Option<i64>,
    hash: Option<String>,
    read_error: Option<String>,
}

impl BlobIdentity {
    /// Say in the row itself that a column is absent because the read failed.
    ///
    /// Without this the row looks like a complete record of an item whose bytes
    /// are gone, which is a different — and much more final — verdict than "the
    /// archive could not be read at that moment"; the second is worth retrying,
    /// the first is not.
    fn annotate(&self, detail: String) -> String {
        match &self.read_error {
            Some(problems) => format!("{detail}; blob metadata unreadable ({problems})"),
            None => detail,
        }
    }
}

/// Turn the two archive reads a quarantine row depends on into columns, keeping
/// a read that FAILED apart from a blob that is genuinely GONE.
///
/// Both columns are nullable and a null hash is read as "the bytes are no longer
/// in the archive", so a transient SQLITE_BUSY on the way to `SELECT hash` would
/// otherwise persist that claim about an archive whose bytes are sitting right
/// there — and lose the blake3 that correlates this item with the target's
/// de-duplication marker without a word about why. The failure is therefore
/// logged as it happens and carried into `error_detail`, so the row admits which
/// of its fields are absent for that reason.
///
/// The row is still written: a partial row beats no row, and an archive unwell
/// enough to fail these reads will normally fail the quarantine WRITE too, which
/// `Quarantine::note_write_error` already escalates into a run-level signal.
fn blob_identity(
    len: Result<Option<u64>, rusqlite::Error>,
    hash: Result<Option<String>, rusqlite::Error>,
    ty: ObjectType,
    blob_local_id: i64,
    logger: &Logger,
) -> BlobIdentity {
    let mut problems: Vec<String> = Vec::new();
    let size_bytes = match len {
        Ok(n) => n.map(|n| n as i64),
        Err(e) => {
            problems.push(format!("size: {e}"));
            None
        }
    };
    let hash = match hash {
        Ok(h) => h,
        Err(e) => {
            problems.push(format!("blake3: {e}"));
            None
        }
    };
    if problems.is_empty() {
        return BlobIdentity {
            size_bytes,
            hash,
            read_error: None,
        };
    }
    let joined = problems.join("; ");
    logger.error(&format!(
        "{}: reading blob #{blob_local_id} back from the archive failed ({joined}); \
         the quarantine row is recorded without those fields",
        ty.jmap_name()
    ));
    BlobIdentity {
        size_bytes,
        hash,
        read_error: Some(joined),
    }
}

/// Tell an orphaned de-duplication marker apart from a blob store that is simply
/// down.
///
/// A content-addressed target de-duplicates an upload by content hash: it
/// answers 200 with a blobId without writing the bytes again, because its
/// metadata store already holds a marker for that hash. If the bytes behind that
/// marker were lost, the marker keeps shadowing them — the upload keeps
/// succeeding, the create keeps reporting `blobNotFound`, and re-uploading the
/// SAME bytes produces the SAME hash and is de-duped away again, so the
/// condition cannot self-heal no matter how many attempts are made.
///
/// The distinguishing observation is therefore: the target accepted the upload
/// and named a blobId, yet cannot serve that blobId back. Anything else — a
/// blobId it can serve, a lookup that errors out, a target without the blob
/// extension — is recorded as what it is rather than folded into the orphan
/// verdict, because those call for very different responses.
fn classify_blob(net: &Net, blob_id: &str) -> (String, Option<String>) {
    match blobxfer::lookup_blob(&net.client, &net.session, &net.account, blob_id) {
        Ok(BlobLookup::NotFound) => (PROBE_ORPHANED_MARKER.to_owned(), None),
        Ok(BlobLookup::Found { size }) => (
            PROBE_RETRIEVABLE.to_owned(),
            size.map(|n| format!("Blob/get reports {n} bytes for {blob_id}")),
        ),
        Ok(BlobLookup::Unsupported) => (PROBE_UNSUPPORTED.to_owned(), None),
        Err(e) => (PROBE_STORE_UNAVAILABLE.to_owned(), Some(e.to_string())),
    }
}

fn dedup_note(first_blob: &str, second_blob: &str) -> &'static str {
    if first_blob == second_blob {
        "; the re-upload returned the same blobId, so the target de-duplicated it by content hash"
    } else {
        ""
    }
}

fn probe_note(probe: &str) -> &'static str {
    match probe {
        PROBE_ORPHANED_MARKER => {
            "; Blob/get cannot serve that blobId back, so the upload was de-duplicated onto a \
             marker whose bytes are gone from the target blob store and re-uploading identical \
             bytes can never recover it"
        }
        PROBE_RETRIEVABLE => {
            "; Blob/get can still serve that blobId, so the refusal did not come from a missing blob"
        }
        PROBE_STORE_UNAVAILABLE => {
            "; the Blob/get probe itself failed, which points at an unhealthy target blob store \
             rather than at this one item"
        }
        PROBE_UNSUPPORTED => {
            "; the target does not offer urn:ietf:params:jmap:blob, so retrievability could not \
             be established"
        }
        _ => "",
    }
}

impl BlobUpload for Uploader<'_> {
    fn upload(&mut self, local_id: i64) -> Result<JmapId, JmapError> {
        self.upload_with(local_id, "application/octet-stream")
    }
}

#[derive(Clone)]
struct Net {
    client: HttpClient,
    api: String,
    account: String,
    limits: Limits,
    session: Session,
    dry_run: bool,
}

fn has_rows(conn: &Connection, ty: ObjectType) -> bool {
    let table = crate::sync::table_name(ty);
    conn.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table})"), [], |r| {
        r.get::<_, i64>(0)
    })
    .map(|n| n != 0)
    .unwrap_or(false)
}

pub fn run(common: CommonConfig, config: ExportConfig) -> Result<Summary, Error> {
    let logger = common.logger;
    let ctx = Context::open(common, &config.connect)?;
    // Before anything reads the quarantine — and before the target is even
    // contacted, since this is archive housekeeping that owes the target
    // nothing — drop the rows whose object has gone from the archive, so
    // `Quarantine::open` below starts from a set that still means something.
    Quarantine::reap_stale(&ctx.conn, ctx.dry_run(), &logger)?;
    let connected = connect::prepare(&ctx, &config.connect)?;

    let net = Net {
        client: ctx.client.clone(),
        api: connected.session.api_url.clone(),
        account: connected.account_id.clone(),
        limits: connected.limits,
        session: connected.session.clone(),
        dry_run: ctx.dry_run(),
    };

    let work = work_list(&ctx.conn, &config, &connected, &logger);
    let mut maps = Maps::default();
    let mut summary = Summary::default();
    let mut dry_rows: Vec<(&'static str, u64, u64, u64)> = Vec::new();
    let mut plans: HashMap<ObjectType, Plan> = HashMap::new();
    let mut counts_per_type: HashMap<ObjectType, TypeCounts> = HashMap::new();

    for ty in &work {
        if logger.enabled(LEVEL_DEFAULT) {
            eprintln!("export: {} ...", ty.jmap_name());
        }
        let mut counts = TypeCounts::default();
        let res = reconcile_type(
            &ctx,
            &net,
            *ty,
            &mut maps,
            &logger,
            &mut counts,
            &mut dry_rows,
            &mut summary.unrecorded_failures,
        );
        let plan = match res {
            Ok(p) => p,
            // The type gave up part way through, so its counters describe less
            // than the type holds; the one `failed` unit stands for the whole
            // aborted pass. A bookkeeping error never arrives here — it would be
            // counted as an object the target refused, which it is not. See
            // `Quarantine::finish`.
            Err(e) => {
                logger.warn(&format!("type {} aborted: {e}", ty.jmap_name()));
                counts.failed += 1;
                Plan::default()
            }
        };
        plans.insert(*ty, plan);
        counts_per_type.insert(*ty, counts);
    }

    if config.prune {
        prune_phase(
            &ctx,
            &net,
            &work,
            &plans,
            &config,
            &logger,
            &mut counts_per_type,
        )?;
    }

    for ty in &work {
        if let Some(counts) = counts_per_type.remove(ty) {
            summary.per_type.push((ty.jmap_name(), counts));
        }
    }

    if ctx.dry_run() {
        print_dry_run(&dry_rows, config.prune);
        return Ok(Summary::default());
    }
    summary.retries_observed = ctx.client.retries_observed();
    summary.retry_after_sleeps = ctx.client.retry_after_sleeps();
    Ok(summary)
}

fn prune_phase(
    ctx: &Context,
    net: &Net,
    work: &[ObjectType],
    plans: &HashMap<ObjectType, Plan>,
    config: &ExportConfig,
    logger: &Logger,
    counts_per_type: &mut HashMap<ObjectType, TypeCounts>,
) -> Result<(), Error> {
    let totals: Vec<(ObjectType, &Plan)> = work
        .iter()
        .filter_map(|ty| plans.get(ty).map(|p| (*ty, p)))
        .filter(|(_, p)| !p.prune_candidates.is_empty())
        .collect();
    if totals.is_empty() {
        return Ok(());
    }
    eprintln!("prune plan:");
    let total: usize = totals.iter().map(|(_, p)| p.prune_candidates.len()).sum();
    for (ty, p) in &totals {
        eprintln!(
            "  {:<22} {:>6} candidate(s); sample: {}",
            ty.jmap_name(),
            p.prune_candidates.len(),
            sample(&p.prune_candidates),
        );
    }
    eprintln!("  {:<22} {:>6} total", "(all types)", total);
    if ctx.dry_run() {
        return Ok(());
    }
    if !config.yes && std::io::stdin().is_terminal() {
        eprint!("destroy all {total} objects across all types? [y/N] ");
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .map_err(|e| Error::Partial(e.to_string()))?;
        if !matches!(line.trim(), "y" | "Y" | "yes") {
            return Err(Error::PruneAborted);
        }
    }
    for ty in work.iter().rev() {
        if let Some(plan) = plans.get(ty)
            && !plan.prune_candidates.is_empty()
            && let Some(counts) = counts_per_type.get_mut(ty)
        {
            do_destroy(net, *ty, plan, logger, counts);
        }
    }
    Ok(())
}

fn work_list(
    conn: &Connection,
    config: &ExportConfig,
    connected: &Connected,
    logger: &Logger,
) -> Vec<ObjectType> {
    let selected = config.objects.as_ref();
    EXPORT_ORDER
        .into_iter()
        .filter(|ty| selected.map(|s| s.contains(ty)).unwrap_or(true))
        .filter(|ty| has_rows(conn, *ty))
        .filter(|ty| {
            if connected.supports(*ty) {
                true
            } else {
                logger.warn(&format!(
                    "target does not support {}; skipping",
                    ty.jmap_name()
                ));
                false
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn reconcile_type(
    ctx: &Context,
    net: &Net,
    ty: ObjectType,
    maps: &mut Maps,
    logger: &Logger,
    counts: &mut TypeCounts,
    dry_rows: &mut Vec<(&'static str, u64, u64, u64)>,
    unrecorded: &mut Vec<String>,
) -> Result<Plan, Error> {
    let plan = match ty {
        ObjectType::Mailbox | ObjectType::FileNode => {
            tree::reconcile(ctx, net, ty, maps, counts, logger, unrecorded)
        }
        ObjectType::AddressBook | ObjectType::Calendar => {
            flat::reconcile(ctx, net, ty, maps, counts, logger, unrecorded)
        }
        ObjectType::Identity => keyed::reconcile_identity(ctx, net, maps, counts, logger),
        ObjectType::ParticipantIdentity => {
            keyed::reconcile_participant(ctx, net, maps, counts, logger)
        }
        ObjectType::SieveScript => sieve::reconcile(ctx, net, maps, counts, logger, unrecorded),
        ObjectType::ContactCard | ObjectType::CalendarEvent => {
            uidtype::reconcile(ctx, net, ty, maps, counts, logger, unrecorded)
        }
        ObjectType::Email => email::reconcile(ctx, net, maps, counts, logger, unrecorded),
    }?;

    if ctx.dry_run() {
        dry_rows.push((
            ty.jmap_name(),
            counts.created,
            counts.skipped,
            plan.prune_candidates.len() as u64,
        ));
    }

    Ok(plan)
}

#[derive(Default)]
pub struct Plan {
    pub prune_candidates: Vec<String>,
    pub active_sieve_target: Option<String>,
}

fn do_destroy(net: &Net, ty: ObjectType, plan: &Plan, logger: &Logger, counts: &mut TypeCounts) {
    if ty == ObjectType::SieveScript {
        deactivate_active_sieve_script(net, logger);
    }
    let destroy = Value::Array(
        plan.prune_candidates
            .iter()
            .map(|s| Value::String(s.clone()))
            .collect(),
    );
    let extra = destroy_contents_arg(ty);
    match set_call(
        &net.client,
        &net.api,
        &net.account,
        ty.jmap_name(),
        SetRequest {
            destroy: Some(destroy),
            extra_args: &extra,
            ..Default::default()
        },
        &net.limits,
    ) {
        Ok(outcome) => {
            counts.deleted += outcome.destroyed.len() as u64;
            for (id, err) in &outcome.not_destroyed {
                logger.warn(&format!(
                    "prune: {} {id} not destroyed: {err}",
                    ty.jmap_name()
                ));
                counts.skipped += 1;
            }
        }
        Err(e) => {
            logger.warn(&format!(
                "prune {}: destroy request failed: {e}",
                ty.jmap_name()
            ));
            counts.failed += plan.prune_candidates.len() as u64;
        }
    }
}

fn deactivate_active_sieve_script(net: &Net, logger: &Logger) {
    let mut req = Request::new();
    req.call(
        "SieveScript/set",
        json!({ "accountId": net.account, "onSuccessDeactivateScript": true }),
        "d",
    );
    let outcome = req.send(&net.client, &net.api).and_then(|resp| {
        let mr = resp.first()?;
        crate::jmap::request::check_method_error(mr)
    });
    if let Err(e) = outcome {
        logger.warn(&format!(
            "prune: SieveScript deactivation failed before destroy: {e}"
        ));
    }
}

fn destroy_contents_arg(ty: ObjectType) -> Vec<(&'static str, Value)> {
    match ty {
        ObjectType::AddressBook => vec![("onDestroyRemoveContents", Value::Bool(false))],
        ObjectType::Calendar => vec![("onDestroyRemoveEvents", Value::Bool(false))],
        ObjectType::FileNode => vec![("onDestroyRemoveChildren", Value::Bool(false))],
        _ => Vec::new(),
    }
}

fn sample(ids: &[String]) -> String {
    let n = ids.len().min(5);
    ids[..n].join(", ")
}

fn print_dry_run(rows: &[(&'static str, u64, u64, u64)], prune: bool) {
    if prune {
        println!(
            "{:<22} {:>10} {:>10} {:>12}",
            "TYPE", "CREATE", "MATCHED", "WOULD-DESTROY"
        );
        for (ty, c, m, d) in rows {
            println!("{ty:<22} {c:>10} {m:>10} {d:>12}");
        }
    } else {
        println!("{:<22} {:>10} {:>10}", "TYPE", "CREATE", "MATCHED");
        for (ty, c, m, _) in rows {
            println!("{ty:<22} {c:>10} {m:>10}");
        }
    }
}

mod tree;

mod flat;

mod keyed;

mod sieve;

mod uidtype;

mod email;

mod common {
    use super::*;

    pub fn target_query_get(
        net: &Net,
        ty: ObjectType,
        props: Option<&[&str]>,
    ) -> Result<Vec<Value>, JmapError> {
        let ids = query_all_ids(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            &net.limits,
        )?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let got = get_objects::<Value>(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            &ids,
            props,
            &net.limits,
        )?;
        Ok(got.list)
    }

    pub fn target_get_all(net: &Net, ty: ObjectType) -> Result<Vec<Value>, JmapError> {
        Ok(get_all::<Value>(&net.client, &net.api, &net.account, ty.jmap_name())?.list)
    }

    pub fn jid(v: &Value) -> Option<String> {
        v.get("id").and_then(Value::as_str).map(str::to_owned)
    }

    pub fn create_batch(
        net: &Net,
        ty: ObjectType,
        creates: Vec<(String, Value)>,
    ) -> Result<crate::jmap::request::SetOutcome, JmapError> {
        if net.dry_run {
            return Ok(synthesize_dry_run_outcome(ty, &creates));
        }
        let mut map = Map::new();
        for (cid, obj) in creates {
            map.insert(cid, obj);
        }
        set_call(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            SetRequest {
                create: Some(Value::Object(map)),
                ..Default::default()
            },
            &net.limits,
        )
    }

    fn blob_not_found(outcome: &crate::jmap::request::SetOutcome, cid: &str) -> bool {
        outcome.not_created.iter().any(|(c, err)| {
            c == cid && err.get("type").and_then(Value::as_str) == Some("blobNotFound")
        })
    }

    /// The one blob an object was built from, when there is exactly one.
    ///
    /// `notCreated` names no blob, so an object assembled from several of them
    /// gives no way to attribute the refusal to any single one. Only the
    /// unambiguous case is reported; otherwise the quarantine leaves those
    /// columns NULL rather than pointing at whichever blob happened to be first.
    pub fn sole_blob(touched: &[i64]) -> Option<i64> {
        match touched {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// Re-upload and retry once when the target says the blob is not there.
    ///
    /// The second return value carries the pair of blobIds the retry saw, so the
    /// caller can hand it to `Quarantine::record_refusal` if the retry did not
    /// help: the retry re-sends identical bytes, so a second blobId equal to the
    /// first is the target telling us it de-duplicated by content hash instead of
    /// storing anything, which is the whole reason the retry cannot self-heal an
    /// orphaned marker. It is `None` unless exactly one blob is involved — see
    /// `sole_blob`.
    pub fn retry_if_blob_missing<F>(
        net: &Net,
        ty: ObjectType,
        cid: &str,
        uploader: &mut Uploader<'_>,
        touched: &[i64],
        outcome: crate::jmap::request::SetOutcome,
        mut rebuild: F,
    ) -> Result<(crate::jmap::request::SetOutcome, Option<BlobRetry>), Error>
    where
        F: FnMut(&mut Uploader<'_>) -> Result<Value, Error>,
    {
        if !blob_not_found(&outcome, cid) {
            return Ok((outcome, None));
        }
        // Read before the invalidation below drops it: this is the blobId the
        // target named for the upload it has just refused to resolve.
        let first = sole_blob(touched).and_then(|id| uploader.cached(id));
        for id in touched {
            uploader.invalidate(*id);
        }
        let _ = uploader.take_touched();
        let wire = rebuild(uploader)?;
        let _ = uploader.take_touched();
        let retry = match (first, sole_blob(touched).and_then(|id| uploader.cached(id))) {
            (Some(first), Some(second)) => Some(BlobRetry { first, second }),
            _ => None,
        };
        let outcome = create_batch(net, ty, vec![(cid.to_owned(), wire)]).map_err(Error::from)?;
        Ok((outcome, retry))
    }

    fn synthesize_dry_run_outcome(
        ty: ObjectType,
        creates: &[(String, Value)],
    ) -> crate::jmap::request::SetOutcome {
        let mut outcome = crate::jmap::request::SetOutcome::default();
        for (cid, _) in creates {
            let synthetic = serde_json::json!({
                "id": format!("dryrun-{}-{cid}", ty.jmap_name())
            });
            outcome.created.push((cid.clone(), synthetic));
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::LEVEL_QUIET;

    /// A real error off the real query rather than a hand-built variant: what
    /// matters is the shape the archive itself produces when the read cannot be
    /// answered, which is indistinguishable from a busy or damaged database.
    fn read_error() -> rusqlite::Error {
        let c = Connection::open_in_memory().unwrap();
        crate::db::blobs::blob_hash_hex(&c, 1).expect_err("no schema, so the read must fail")
    }

    fn quiet() -> Logger {
        Logger::new(LEVEL_QUIET)
    }

    #[test]
    fn a_failed_hash_read_is_kept_and_never_passed_off_as_a_null() {
        let out = blob_identity(
            Ok(Some(4096)),
            Err(read_error()),
            ObjectType::Email,
            7,
            &quiet(),
        );
        assert_eq!(out.size_bytes, Some(4096), "the read that worked is kept");
        assert_eq!(out.hash, None, "there is no hash to record");
        let problems = out
            .read_error
            .clone()
            .expect("the read failure must survive");
        assert!(problems.contains("blake3: "), "got {problems}");
        assert!(
            out.annotate("refused".to_owned())
                .contains("blob metadata unreadable"),
            "the row has to say why the hash is absent"
        );
    }

    #[test]
    fn a_blob_that_is_genuinely_absent_is_not_reported_as_a_failed_read() {
        let out = blob_identity(Ok(None), Ok(None), ObjectType::Email, 7, &quiet());
        assert_eq!(out.size_bytes, None);
        assert_eq!(out.hash, None);
        assert_eq!(
            out.read_error, None,
            "nothing failed; the blob is just gone"
        );
        assert_eq!(out.annotate("refused".to_owned()), "refused");
    }

    #[test]
    fn both_reads_failing_are_reported_separately() {
        let out = blob_identity(
            Err(read_error()),
            Err(read_error()),
            ObjectType::SieveScript,
            7,
            &quiet(),
        );
        let problems = out.read_error.expect("both failures must survive");
        assert!(problems.contains("size: "), "got {problems}");
        assert!(problems.contains("blake3: "), "got {problems}");
    }

    /// The blob columns are only filled in when the refusal can be pinned on one
    /// blob; `notCreated` names none, so several would be a guess.
    #[test]
    fn only_a_single_blob_is_attributed_to_a_refusal() {
        assert_eq!(common::sole_blob(&[7]), Some(7));
        assert_eq!(common::sole_blob(&[]), None);
        assert_eq!(common::sole_blob(&[7, 8]), None);
    }
}
