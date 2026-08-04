/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::path::{Path, PathBuf};

use mockito::Matcher;
use serde_json::{Value, json};
use vandelay::db;
use vandelay::jmap::account::AccountSelector;
use vandelay::jmap::http::Auth;
use vandelay::logging::Logger;
use vandelay::sync::{self, CommonConfig, ConnectConfig, ExportConfig, ImportConfig};
use vandelay::types::ObjectType;

fn tmp() -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "vandelay-mocksync-{}-{:?}-{n}.sqlite",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_file(&p);
    p
}

fn session_body(base: &str) -> String {
    json!({
        "apiUrl": format!("{base}/jmap/api"),
        "uploadUrl": format!("{base}/jmap/upload/{{accountId}}/"),
        "downloadUrl": format!("{base}/jmap/dl/{{accountId}}/{{blobId}}/{{type}}/{{name}}"),
        "capabilities": { "urn:ietf:params:jmap:core": {
            "maxObjectsInGet": 500, "maxObjectsInSet": 500, "maxCallsInRequest": 16,
            "maxConcurrentRequests": 4, "maxConcurrentUpload": 4,
            "maxSizeRequest": 10000000, "maxSizeUpload": 50000000
        } },
        "accounts": { "w": { "name": "alice",
            "accountCapabilities": { "urn:ietf:params:jmap:mail": {} } } }
    })
    .to_string()
}

#[test]
fn export_email_already_exists_is_matched_not_failed() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(
            &conn,
            b"From: a@x\r\nSubject: hi\r\nMessage-ID: <m-1@h>\r\n\r\nbody",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
             VALUES (?1,'2020-01-01T00:00:00Z','[1]','[\"$seen\"]')",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();

    let _mq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
            {"id":"t1","name":"Inbox","role":"inbox","parentId":null,
             "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UPLOADED"}).to_string())
        .expect(1)
        .create();
    let _imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"alreadyExists","existingId":"x9"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        CommonConfig {
            archive: archive.clone(),
            threads: 1,
            dry_run: false,
            max_retries: 1,
            allow_invalid_certs: false,
            logger: Logger::from_flags(true, 0),
        },
        ExportConfig {
            connect: ConnectConfig {
                url: base.clone(),
                auth: Auth::Basic {
                    user: "u".into(),
                    password: "p".into(),
                },
                account: AccountSelector::Id("w".into()),
            },
            objects: None,
            prune: false,
            yes: true,
        },
    )
    .expect("export run");

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.created, 0);
    assert_eq!(email.failed, 0, "alreadyExists must not be a failure");
    assert_eq!(email.skipped, 1, "alreadyExists folds into matched");
    assert!(!summary.any_failed());

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_mailbox_name_collision_merges_without_create() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES
                 (1,'Junk Email',NULL,'junk'),
                 (2,'Junk Mail',NULL,NULL)",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(
            &conn,
            b"From: a@x\r\nSubject: spam\r\nMessage-ID: <m-2@h>\r\n\r\nbody",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
             VALUES (?1,'2020-01-01T00:00:00Z','[2]','[\"$seen\"]')",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();

    let _mq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":["c"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
            {"id":"c","name":"Junk Mail","role":"junk","parentId":null,
             "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let no_set = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/set".into()))
        .expect(0)
        .create();

    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UPLOADED"}).to_string())
        .expect(1)
        .create();
    let import_into_c = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex(r#""c":true"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "created":{"e1":{"id":"E1","blobId":"b","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        CommonConfig {
            archive: archive.clone(),
            threads: 1,
            dry_run: false,
            max_retries: 1,
            allow_invalid_certs: false,
            logger: Logger::from_flags(true, 0),
        },
        ExportConfig {
            connect: ConnectConfig {
                url: base.clone(),
                auth: Auth::Basic {
                    user: "u".into(),
                    password: "p".into(),
                },
                account: AccountSelector::Id("w".into()),
            },
            objects: None,
            prune: false,
            yes: true,
        },
    )
    .expect("export run");

    let mailbox = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mailbox.created, 0, "no mailbox is created");
    assert_eq!(mailbox.failed, 0, "the name collision is not a failure");
    assert_eq!(
        mailbox.skipped, 2,
        "role-matched Junk Email + merged Junk Mail"
    );

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.created, 1, "the Junk Mail email lands on the target");
    assert_eq!(email.failed, 0);
    assert!(!summary.any_failed());

    no_set.assert();
    import_into_c.assert();
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_mailbox_already_exists_maps_existing_id() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Junk Mail',NULL,NULL)",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(
            &conn,
            b"From: a@x\r\nSubject: spam\r\nMessage-ID: <m-3@h>\r\n\r\nbody",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
             VALUES (?1,'2020-01-01T00:00:00Z','[1]','[\"$seen\"]')",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();

    let _mq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
            {"id":"t1","name":"Inbox","role":"inbox","parentId":null,
             "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let set_collides = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/set".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/set",{"accountId":"w",
                "notCreated":{"c1":{"type":"alreadyExists","existingId":"c"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UPLOADED"}).to_string())
        .expect(1)
        .create();
    let import_into_c = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex(r#""c":true"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "created":{"e1":{"id":"E1","blobId":"b","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        CommonConfig {
            archive: archive.clone(),
            threads: 1,
            dry_run: false,
            max_retries: 1,
            allow_invalid_certs: false,
            logger: Logger::from_flags(true, 0),
        },
        ExportConfig {
            connect: ConnectConfig {
                url: base.clone(),
                auth: Auth::Basic {
                    user: "u".into(),
                    password: "p".into(),
                },
                account: AccountSelector::Id("w".into()),
            },
            objects: None,
            prune: false,
            yes: true,
        },
    )
    .expect("export run");

    let mailbox = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mailbox.created, 0, "the create collided");
    assert_eq!(
        mailbox.failed, 0,
        "alreadyExists on Mailbox/set is not a failure"
    );
    assert_eq!(mailbox.skipped, 1, "existingId folds into matched");

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.created, 1, "the email lands in the existing folder");
    assert_eq!(email.failed, 0);
    assert!(!summary.any_failed());

    set_collides.assert();
    import_into_c.assert();
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn email_export_sends_one_email_per_import_call() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        for n in 1..=2 {
            let raw =
                format!("From: a@x\r\nSubject: m{n}\r\nMessage-ID: <m-{n}@h>\r\n\r\nbody {n}",);
            let blob = db::blobs::intern_blob(&conn, raw.as_bytes()).unwrap();
            conn.execute(
                "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
                 VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]')",
                rusqlite::params![blob],
            )
            .unwrap();
        }
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();

    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _mq_empty = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/query".into()),
            Matcher::Regex("anchor".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
                {"id":"t1","name":"Inbox","role":"inbox","parentId":null,
                 "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let _ups = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();

    let single_only = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex("e1".into()),
            Matcher::Regex("e2".into()),
        ]))
        .expect(0)
        .create();

    let imports = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",
                {"accountId":"w","created":{"e":{"id":"x","blobId":"b","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(2)
        .create();

    let summary = sync::export::run(
        CommonConfig {
            archive: archive.clone(),
            threads: 1,
            dry_run: false,
            max_retries: 0,
            allow_invalid_certs: false,
            logger: Logger::from_flags(true, 0),
        },
        ExportConfig {
            connect: ConnectConfig {
                url: base.clone(),
                auth: Auth::Basic {
                    user: "u".into(),
                    password: "p".into(),
                },
                account: AccountSelector::Id("w".into()),
            },
            objects: None,
            prune: false,
            yes: true,
        },
    )
    .expect("export run");

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.created, 2, "both emails imported in per-item rounds");
    assert_eq!(email.failed, 0, "no per-unit failure");
    assert!(!summary.any_failed(), "no whole-run failure");

    single_only.assert();
    imports.assert();
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blob_not_found_reuploads_and_retries() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        let raw = b"From: a@x\r\nSubject: dup\r\nMessage-ID: <dup-1@h>\r\n\r\nbody";
        let blob = db::blobs::intern_blob(&conn, raw).unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
                 VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]')",
                rusqlite::params![blob],
            )
            .unwrap();
        }
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();

    let _mq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
            {"id":"t1","name":"Inbox","role":"inbox","parentId":null,
             "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let up1 = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(1)
        .create();
    let up2 = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP2"}).to_string())
        .expect(1)
        .create();

    let imp_e1 = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex("e1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "created":{"e1":{"id":"x1","blobId":"UP1","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let imp_e2_stale = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex("e2".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e2":{"type":"blobNotFound"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let imp_e2_fresh = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/import".into()),
            Matcher::Regex("e2".into()),
            Matcher::Regex("UP2".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "created":{"e2":{"id":"x2","blobId":"UP2","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        CommonConfig {
            archive: archive.clone(),
            threads: 1,
            dry_run: false,
            max_retries: 1,
            allow_invalid_certs: false,
            logger: Logger::from_flags(true, 0),
        },
        ExportConfig {
            connect: ConnectConfig {
                url: base.clone(),
                auth: Auth::Basic {
                    user: "u".into(),
                    password: "p".into(),
                },
                account: AccountSelector::Id("w".into()),
            },
            objects: None,
            prune: false,
            yes: true,
        },
    )
    .expect("export run");

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.created, 2, "both emails end up created");
    assert_eq!(email.failed, 0, "blobNotFound self-heals, not a failure");
    assert_eq!(email.skipped, 0);
    assert!(!summary.any_failed());

    up1.assert();
    up2.assert();
    imp_e1.assert();
    imp_e2_stale.assert();
    imp_e2_fresh.assert();
    let _ = std::fs::remove_file(&archive);
}

fn session_body_full(base: &str) -> String {
    json!({
        "apiUrl": format!("{base}/jmap/api"),
        "uploadUrl": format!("{base}/jmap/upload/{{accountId}}/"),
        "downloadUrl": format!("{base}/jmap/dl/{{accountId}}/{{blobId}}/{{type}}/{{name}}"),
        "capabilities": { "urn:ietf:params:jmap:core": {
            "maxObjectsInGet": 500, "maxObjectsInSet": 500, "maxCallsInRequest": 16,
            "maxConcurrentRequests": 4, "maxConcurrentUpload": 4,
            "maxSizeRequest": 10000000, "maxSizeUpload": 50000000
        } },
        "accounts": { "w": { "name": "alice",
            "accountCapabilities": {
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:sieve": {},
                "urn:ietf:params:jmap:contacts": {},
                "urn:ietf:params:jmap:calendars": {},
                "urn:ietf:params:jmap:filenode": {}
            } } }
    })
    .to_string()
}

fn import_cfg_objects(base: &str, objects: Vec<ObjectType>) -> ImportConfig {
    ImportConfig {
        connect: ConnectConfig {
            url: base.to_owned(),
            auth: Auth::Basic {
                user: "u".into(),
                password: "p".into(),
            },
            account: AccountSelector::Id("w".into()),
        },
        objects: Some(objects),
        allow_source_change: false,
    }
}

fn export_cfg_objects(base: &str, objects: Vec<ObjectType>) -> ExportConfig {
    ExportConfig {
        connect: ConnectConfig {
            url: base.to_owned(),
            auth: Auth::Basic {
                user: "u".into(),
                password: "p".into(),
            },
            account: AccountSelector::Id("w".into()),
        },
        objects: Some(objects),
        prune: false,
        yes: true,
    }
}

fn common(archive: &Path) -> CommonConfig {
    CommonConfig {
        archive: archive.to_path_buf(),
        threads: 1,
        dry_run: false,
        max_retries: 1,
        allow_invalid_certs: false,
        logger: Logger::from_flags(true, 0),
    }
}

fn common_dry(archive: &Path) -> CommonConfig {
    CommonConfig {
        dry_run: true,
        ..common(archive)
    }
}

fn anchor_terminator(server: &mut mockito::Server, api: &str, type_name: &str) -> mockito::Mock {
    server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex(format!("{type_name}/query")),
            Matcher::Regex("\"anchor\"".into()),
        ]))
        .with_body(
            json!({"methodResponses":[[format!("{type_name}/query"),
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_most(1024)
        .create()
}

#[test]
fn import_removes_vanished_mailbox_from_archive_on_second_pass() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["A","B","C"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s1","list":[
                {"id":"A","name":"alpha","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true},
                {"id":"B","name":"bravo","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true},
                {"id":"C","name":"charlie","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let s1 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("first import");
    let mb1 = s1
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mb1.fetched, 3, "first pass fetched all three mailboxes");
    {
        let conn = rusqlite::Connection::open(&archive).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM mailboxes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);
    }

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["A","C"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _ch2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/changes",{"accountId":"w","oldState":"s1",
                "newState":"s2","hasMoreChanges":false,"created":[],"updated":[],"destroyed":[]},"c"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("second import");
    let mb2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mb2.fetched, 0, "second pass fetches nothing");
    assert_eq!(mb2.updated, 0, "no changed mailboxes reported");
    assert_eq!(mb2.deleted, 1, "vanished mailbox B is deleted");
    {
        let conn = rusqlite::Connection::open(&archive).unwrap();
        let names: Vec<String> = conn
            .prepare("SELECT name FROM mailboxes ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(names, vec!["alpha".to_owned(), "charlie".to_owned()]);
    }
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_present_item_change_is_propagated_via_changes() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["A"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s1","list":[
                {"id":"A","name":"OriginalName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("first import");
    {
        let conn = rusqlite::Connection::open(&archive).unwrap();
        let cursor: String = conn
            .query_row(
                "SELECT state FROM sync_state_jmap WHERE type_name='Mailbox'",
                [],
                |r| r.get(0),
            )
            .expect("first import records the state cursor");
        assert_eq!(cursor, "s1");
    }

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["A"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let changes = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/changes".into()),
            Matcher::Regex("\"sinceState\":\"s1\"".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Mailbox/changes",{"accountId":"w","oldState":"s1",
                "newState":"s2","hasMoreChanges":false,"created":[],"updated":["A"],"destroyed":[]},"c"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s2","list":[
                {"id":"A","name":"UpdatedName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("second import");
    changes.assert();
    let mb2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mb2.fetched, 0, "no new objects on the second pass");
    assert_eq!(
        mb2.updated, 1,
        "the changed mailbox is detected via /changes and refreshed in place"
    );
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let name: String = conn
        .query_row("SELECT name FROM mailboxes WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        name, "UpdatedName",
        "a server-side property change is propagated into the archive"
    );
    let cursor: String = conn
        .query_row(
            "SELECT state FROM sync_state_jmap WHERE type_name='Mailbox'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cursor, "s2", "cursor advances to the changes newState");
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_removes_vanished_email_and_drops_cross_ref() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");

    let _mbq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["MX"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mbg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"sm1","list":[
                {"id":"MX","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":["E1","E2"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se1","list":[
                {"id":"E1","blobId":"BLB1","receivedAt":"2020-01-01T00:00:00Z","mailboxIds":{"MX":true},"keywords":{"$seen":true}},
                {"id":"E2","blobId":"BLB2","receivedAt":"2020-01-02T00:00:00Z","mailboxIds":{"MX":true},"keywords":{}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _dl1 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB1/.*".into()))
        .with_body("From: a@x\r\nMessage-ID: <1@h>\r\n\r\nbody-one")
        .expect(1)
        .create();
    let _dl2 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB2/.*".into()))
        .with_body("From: b@x\r\nMessage-ID: <2@h>\r\n\r\nbody-two")
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("first import");
    {
        let conn = rusqlite::Connection::open(&archive).unwrap();
        assert_eq!(
            conn.query_row::<i64, _, _>("SELECT count(*) FROM emails", [], |r| r.get(0))
                .unwrap(),
            2
        );
    }

    let _mbq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["MX"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":["E2"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _mbch2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/changes",{"accountId":"w","oldState":"sm1",
                "newState":"sm2","hasMoreChanges":false,"created":[],"updated":[],"destroyed":[]},"c"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _ech2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/changes".into()))
        .with_body(
            json!({"methodResponses":[["Email/changes",{"accountId":"w","oldState":"se1",
                "newState":"se2","hasMoreChanges":false,"created":[],"updated":[],"destroyed":[]},"c"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("second import");
    let em2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(em2.deleted, 1, "vanished email is deleted from archive");
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let remaining: i64 = conn
        .query_row("SELECT count(*) FROM emails", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 1, "only the still-present email remains");
    let blobs: i64 = conn
        .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(blobs, 1, "blob GC reclaims orphan blob of deleted email");
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_missing_email_blob_is_skipped_and_counted_once() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");

    let _mbq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["MX"]},"q"]]})
            .to_string(),
        )
        .create();
    let _mbg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"sm1","list":[
                {"id":"MX","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":["E1","E2"]},"q"]]})
            .to_string(),
        )
        .create();
    let _eg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se1","list":[
                {"id":"E1","blobId":"BLB1","receivedAt":"2020-01-01T00:00:00Z","mailboxIds":{"MX":true},"keywords":{"$seen":true}},
                {"id":"E2","blobId":"BLB2","receivedAt":"2020-01-02T00:00:00Z","mailboxIds":{"MX":true},"keywords":{}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .create();
    let _dl1 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB1/.*".into()))
        .with_body("From: a@x\r\nMessage-ID: <1@h>\r\n\r\nbody-one")
        .create();
    let _dl2 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB2/.*".into()))
        .with_status(404)
        .with_body(json!({"status":404,"title":"Not Found"}).to_string())
        .create();

    let summary = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("import does not abort on a missing blob");

    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(
        email.fetched, 1,
        "only the email with a present blob imports"
    );
    assert_eq!(
        email.failed, 1,
        "a missing blob counts the email failed exactly once, not twice"
    );
    assert!(summary.any_failed());

    let conn = rusqlite::Connection::open(&archive).unwrap();
    assert_eq!(
        conn.query_row::<i64, _, _>("SELECT count(*) FROM emails", [], |r| r.get(0))
            .unwrap(),
        1,
        "the skipped email leaves no row"
    );
    assert_eq!(
        conn.query_row::<i64, _, _>(
            "SELECT count(*) FROM sync_id_jmap WHERE type_name='Email'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1,
        "no id mapping is recorded for the skipped email, so a re-run retries it"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_missing_target_email_is_created_on_rerun() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        for n in 1..=2 {
            let raw =
                format!("From: a@x\r\nSubject: m{n}\r\nMessage-ID: <m-{n}@h>\r\n\r\nbody {n}",);
            let blob = db::blobs::intern_blob(&conn, raw.as_bytes()).unwrap();
            let mm = vandelay::sync::keys::index_to_json(
                &vandelay::sync::emailmeta::email_index_from_blob(raw.as_bytes()),
            );
            conn.execute(
                "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords,message_match)
                 VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]', ?2)",
                rusqlite::params![blob, mm],
            )
            .unwrap();
        }
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");

    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["T1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
                {"id":"T1","name":"Inbox","role":"inbox","parentId":null,"myRights":{"mayDelete":true}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":["X1"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","list":[
                {"id":"X1","messageId":["m-1@h"]}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"BUP"}).to_string())
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "created":{"e2":{"id":"Y2","blobId":"BUP","threadId":"t","size":10}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export");
    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.skipped, 1, "Message-ID match with X1 skips it");
    assert_eq!(email.created, 1, "missing email is created");
    assert_eq!(email.failed, 0);
    upload.assert();
    create.assert();
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blake3_fallback_matches_when_no_message_id() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let raw = "From: a@x\r\nSubject: hello\r\n\r\nno-msg-id-body";
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(&conn, raw.as_bytes()).unwrap();
        let mm = vandelay::sync::keys::index_to_json(
            &vandelay::sync::emailmeta::email_index_from_blob(raw.as_bytes()),
        );
        conn.execute(
            "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords,message_match)
             VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]', ?2)",
            rusqlite::params![blob, mm],
        )
        .unwrap();
    }

    let local_idx = vandelay::sync::emailmeta::email_index_from_blob(raw.as_bytes());
    assert!(local_idx.mids.is_empty(), "blob must lack Message-ID");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");
    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["T1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
                {"id":"T1","name":"Inbox","role":"inbox","parentId":null,"myRights":{"mayDelete":true}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":["X1"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eg_min = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/get".into()),
            Matcher::Regex("messageId".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","list":[
                {"id":"X1","messageId":[]}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _eg_full = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Email/get".into()),
            Matcher::Regex("from".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","list":[
                {"id":"X1","messageId":[],"from":[{"email":"a@x"}],"subject":"hello","sentAt":"","to":[]}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let no_upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .expect(0)
        .create();
    let no_import = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .expect(0)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export");
    no_upload.assert();
    no_import.assert();
    let email = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(email.skipped, 1, "BLAKE3 fallback matched target");
    assert_eq!(email.created, 0);
    assert_eq!(email.failed, 0);
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_address_book_creates_only_missing() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO address_books (id,name,description,is_default)
             VALUES (1,'Personal',NULL,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO address_books (id,name,description,is_default)
             VALUES (2,'Work',NULL,0)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();

    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("AddressBook/get".into()))
        .with_body(
            json!({"methodResponses":[["AddressBook/get",{"accountId":"w","list":[
                {"id":"P","name":"personal","isDefault":true,"myRights":{"mayDelete":false}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("AddressBook/set".into()),
            Matcher::Regex("Work".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["AddressBook/set",{"accountId":"w",
                "created":{"c2":{"id":"WID"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::AddressBook]),
    )
    .expect("export");
    create.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "AddressBook")
        .map(|(_, c)| c.clone())
        .expect("address book counts");
    assert_eq!(counts.skipped, 1, "Personal matches existing (case-fold)");
    assert_eq!(counts.created, 1, "Work is created");
    assert_eq!(counts.failed, 0);
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_creates_only_missing() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Family',1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (2,'Team',0)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["F"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/get".into()),
            Matcher::Regex("defaultAlertsWithoutTime".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[
                {"id":"F","name":"family","description":null,"color":null,
                 "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":true,
                 "includeInAvailability":"all","defaultAlertsWithTime":{},
                 "defaultAlertsWithoutTime":{},"timeZone":null,
                 "myRights":{"mayDelete":false}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let update = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#"\"update\":\{\"F\""#.into()),
            Matcher::Regex(r#"\"name\":\"Family\""#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "updated":{"F":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex("Team".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "created":{"c2":{"id":"TID"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export");
    update.assert();
    create.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Calendar")
        .map(|(_, c)| c.clone())
        .expect("calendar counts");
    assert_eq!(counts.updated, 1, "Family metadata casing converges");
    assert_eq!(counts.skipped, 0);
    assert_eq!(counts.created, 1, "Team is created");
    assert_eq!(counts.failed, 0);
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_matched_calendar_clears_nullable_metadata_and_alerts() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Personal',0)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["P"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"P","name":"Personal","description":"stale","color":"#000000",
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"all","defaultAlertsWithTime":{"old":{}},
                "defaultAlertsWithoutTime":{"old":{}},"timeZone":"Europe/Berlin",
                "myRights":{"mayDelete":true}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let update = server
        .mock("POST", api)
        .match_body(Matcher::PartialJson(json!({
            "methodCalls": [["Calendar/set", {
                "update": {"P": {
                    "name": "Personal",
                    "description": null,
                    "color": null,
                    "defaultAlertsWithTime": {},
                    "defaultAlertsWithoutTime": {},
                    "timeZone": null
                }}
            }, "s"]]
        })))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "updated":{"P":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export");
    update.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.updated, 1);
    assert_eq!(counts.created, 0);
    assert_eq!(counts.failed, 0);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_without_declared_source_default_reuses_native_default() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars
                (id,name,description,color,sort_order,is_subscribed,is_visible,is_default,
                 include_in_availability,default_alerts_with_time,
                 default_alerts_without_time,time_zone)
             VALUES (7,'Imported Calendar','Source description','#123456',9,1,0,0,
                     'attending',NULL,NULL,'Europe/Tallinn')",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["TD"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/get".into()),
            Matcher::Regex("defaultAlertsWithTime".into()),
            Matcher::Regex("defaultAlertsWithoutTime".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"TD","name":"Calendar","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":true,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":false}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let update = server
        .mock("POST", api)
        .match_body(Matcher::PartialJson(json!({
            "methodCalls": [["Calendar/set", {
                "accountId": "w",
                "update": {"TD": {
                    "name": "Imported Calendar",
                    "description": "Source description",
                    "color": "#123456",
                    "sortOrder": 9,
                    "isSubscribed": true,
                    "isVisible": false,
                    "includeInAvailability": "attending",
                    "defaultAlertsWithTime": {},
                    "defaultAlertsWithoutTime": {},
                    "timeZone": "Europe/Tallinn"
                }}
            }, "s"]]
        })))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "updated":{"TD":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""create""#.into()),
        ]))
        .expect(0)
        .create();
    let no_default_change = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessSetIsDefault".into()))
        .expect(0)
        .create();
    let no_destroy = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""destroy""#.into()),
        ]))
        .expect(0)
        .create();

    let mut config = export_cfg_objects(&base, vec![ObjectType::Calendar]);
    config.prune = true;
    let summary = sync::export::run(common(&archive), config).expect("export");

    calendar_get.assert();
    update.assert();
    no_create.assert();
    no_default_change.assert();
    no_destroy.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.created, 0, "the native default must be reused");
    assert_eq!(counts.updated, 1);
    assert_eq!(counts.deleted, 0);
    assert_eq!(counts.failed, 0);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_with_multiple_source_defaults_fails_before_writing_target() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES
                (1,'First',1),
                (2,'Second',1)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["TD"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"TD","name":"Calendar","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":true,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":false}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_write = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/set".into()))
        .expect(0)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("the run reports the invalid surface instead of mutating it");

    calendar_get.assert();
    no_write.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.created, 0);
    assert_eq!(counts.updated, 0);
    assert_eq!(counts.deleted, 0);
    assert_eq!(counts.failed, 1, "ambiguous defaults must fail closed");

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_default_calendar_reuses_target_default_updates_metadata_and_prunes_duplicate() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let alerts = json!({
        "reminder": {
            "@type": "Alert",
            "trigger": {
                "@type": "OffsetTrigger",
                "offset": "-PT15M",
                "relativeTo": "start"
            },
            "action": "display"
        }
    });
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars
                (id,name,description,color,sort_order,is_subscribed,is_visible,is_default,
                 include_in_availability,default_alerts_with_time,
                 default_alerts_without_time,time_zone)
             VALUES (1,'Imported Primary','Source description','#123456',42,0,0,1,
                     'attending',?1,NULL,NULL)",
            rusqlite::params![alerts.to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO calendar_events (id,calendar_ids,data)
             VALUES (10,'[1]',?1)",
            rusqlite::params![
                json!({
                    "@type": "Event",
                    "uid": "source-default-event",
                    "title": "Preserved event"
                })
                .to_string()
            ],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["TD","DUP"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let calendars = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/get".into()),
            Matcher::Regex("defaultAlertsWithTime".into()),
            Matcher::Regex("defaultAlertsWithoutTime".into()),
            Matcher::Regex("includeInAvailability".into()),
            Matcher::Regex("isVisible".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[
                {
                    "id":"TD","name":"Calendar","description":"stale",
                    "color":"#000000","sortOrder":1,"isSubscribed":true,
                    "isVisible":true,"isDefault":true,"includeInAvailability":"none",
                    "defaultAlertsWithTime":{"old":{}},
                    "defaultAlertsWithoutTime":{"old":{}},"timeZone":"Europe/Berlin",
                    "myRights":{"mayDelete":false}
                },
                {
                    "id":"DUP","name":"Imported Primary","isDefault":false,
                    "myRights":{"mayDelete":true}
                }
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let update = server
        .mock("POST", api)
        .match_body(Matcher::PartialJson(json!({
            "methodCalls": [["Calendar/set", {
                "accountId": "w",
                "update": {"TD": {
                    "name": "Imported Primary",
                    "description": "Source description",
                    "color": "#123456",
                    "sortOrder": 42,
                    "isSubscribed": false,
                    "isVisible": false,
                    "includeInAvailability": "attending",
                    "defaultAlertsWithTime": alerts.clone(),
                    "defaultAlertsWithoutTime": {},
                    "timeZone": null
                }}
            }, "s"]]
        })))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "updated":{"TD":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""create":"#.into()),
        ]))
        .expect(0)
        .create();
    let no_is_default_property = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""isDefault""#.into()),
        ]))
        .expect(0)
        .create();
    let _event_term = anchor_terminator(&mut server, api, "CalendarEvent");
    let event_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/query".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/query",{
                "accountId":"w","ids":["EV","EV-DUP"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let event_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/get".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/get",{"accountId":"w","list":[{
                "id":"EV","@type":"Event","uid":"source-default-event",
                "title":"Preserved event","calendarIds":{"DUP":true},
                "description":"stale","alerts":{"old":{}},
                "isDraft":false,"useDefaultAlerts":false,
                "baseEventId":"BASE","isOrigin":true,
                "utcStart":"2026-08-04T09:00:00Z",
                "utcEnd":"2026-08-04T10:00:00Z","blobId":"DERIVED"
            },{
                "id":"EV-DUP","@type":"Event","uid":"source-default-event",
                "title":"Duplicate event","calendarIds":{"DUP":true},
                "isDraft":false,"useDefaultAlerts":false
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let event_update = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""calendarIds":\{"TD":true\}"#.into()),
            Matcher::Regex(r#""description":null"#.into()),
            Matcher::Regex(r#""alerts":null"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "updated":{"EV":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_server_owned_event_patch = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""(baseEventId|isOrigin|utcStart|utcEnd|blobId)""#.into()),
        ]))
        .expect(0)
        .create();
    let no_event_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""create""#.into()),
        ]))
        .expect(0)
        .create();
    let destroy_duplicate_event = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""destroy":\["EV-DUP"\]"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "destroyed":["EV-DUP"]},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let destroy_duplicate = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""destroy":\["DUP"\]"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "destroyed":["DUP"]},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let mut config =
        export_cfg_objects(&base, vec![ObjectType::Calendar, ObjectType::CalendarEvent]);
    config.prune = true;
    let summary = sync::export::run(common(&archive), config).expect("export");

    calendars.assert();
    update.assert();
    no_create.assert();
    no_is_default_property.assert();
    event_query.assert();
    event_get.assert();
    event_update.assert();
    no_server_owned_event_patch.assert();
    no_event_create.assert();
    destroy_duplicate_event.assert();
    destroy_duplicate.assert();
    let calendar = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(calendar.created, 0, "the target default is reused");
    assert_eq!(calendar.updated, 1, "all source metadata is applied");
    assert_eq!(calendar.deleted, 1, "same-name duplicate is pruned");
    assert_eq!(calendar.failed, 0);
    let events = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "CalendarEvent")
        .map(|(_, counts)| counts)
        .expect("calendar event counts");
    assert_eq!(events.created, 0);
    assert_eq!(
        events.updated, 1,
        "existing source content is moved from the duplicate to the reused default id"
    );
    assert_eq!(events.deleted, 1, "the unmatched duplicate UID is pruned");
    assert_eq!(events.failed, 0);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_metadata_not_updated_is_counted_and_quarantined() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Imported Primary',1)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["TD"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"TD","name":"Calendar","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":true,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":false}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let refuse = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""update":\{"TD""#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "notUpdated":{"TD":{"type":"forbidden",
                    "description":"calendar preferences are read-only"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export");
    refuse.assert();

    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.failed, 1);
    assert_eq!(counts.created, 0);
    assert_eq!(counts.updated, 0);
    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "the failed update must be attributable");
    assert_eq!(rows[0].type_name, "calendar");
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].client_id, "u1");
    assert_eq!(rows[0].error_type, "forbidden");
    assert!(rows[0].error_detail.contains("read-only"));

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_event_update_refusal_is_counted_and_quarantined() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Personal',0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO calendar_events (id,calendar_ids,data)
             VALUES (10,'[1]',?1)",
            rusqlite::params![
                json!({"@type":"Event","uid":"event-refusal","title":"Source"}).to_string()
            ],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["C"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"C","name":"Personal","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":true}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _event_term = anchor_terminator(&mut server, api, "CalendarEvent");
    let _event_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/query".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/query",{
                "accountId":"w","ids":["EV"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _event_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/get".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/get",{"accountId":"w","list":[{
                "id":"EV","@type":"Event","uid":"event-refusal","title":"Stale",
                "calendarIds":{"C":true},"isDraft":false,"useDefaultAlerts":false,
                "mayInviteSelf":false,"mayInviteOthers":false,"hideAttendees":false
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let refuse = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""update":\{"EV""#.into()),
            Matcher::Regex(r#""title":"Source""#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "notUpdated":{"EV":{"type":"forbidden",
                    "description":"event is read-only"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar, ObjectType::CalendarEvent]),
    )
    .expect("export");
    refuse.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "CalendarEvent")
        .map(|(_, counts)| counts)
        .expect("event counts");
    assert_eq!(counts.failed, 1);
    assert_eq!(counts.updated, 0);
    assert_eq!(counts.created, 0);
    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].type_name, "calendarevent");
    assert_eq!(rows[0].local_id, 10);
    assert_eq!(rows[0].client_id, "u10");
    assert_eq!(rows[0].error_type, "forbidden");
    assert!(rows[0].error_detail.contains("event is read-only"));

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_calendar_event_update_blob_not_found_reuploads_and_retries() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Personal',0)",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(&conn, b"event attachment").unwrap();
        conn.execute(
            "INSERT INTO calendar_events (id,calendar_ids,data)
             VALUES (10,'[1]',?1)",
            rusqlite::params![
                json!({
                    "@type":"Event","uid":"event-blob-retry","title":"Source",
                    "links":{"attachment":{"@type":"Link","@blob":blob}}
                })
                .to_string()
            ],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["C"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"C","name":"Personal","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":true}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _event_term = anchor_terminator(&mut server, api, "CalendarEvent");
    let _event_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/query".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/query",{
                "accountId":"w","ids":["EV"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _event_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/get".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/get",{"accountId":"w","list":[{
                "id":"EV","@type":"Event","uid":"event-blob-retry","title":"Source",
                "calendarIds":{"C":true},"isDraft":false,"useDefaultAlerts":false
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let upload_stale = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(1)
        .create();
    let upload_fresh = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP2"}).to_string())
        .expect(1)
        .create();
    let stale = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "notUpdated":{"EV":{"type":"blobNotFound"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let fresh = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex("UP2".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "updated":{"EV":null}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar, ObjectType::CalendarEvent]),
    )
    .expect("export");
    upload_stale.assert();
    upload_fresh.assert();
    stale.assert();
    fresh.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "CalendarEvent")
        .map(|(_, counts)| counts)
        .expect("event counts");
    assert_eq!(counts.updated, 1);
    assert_eq!(counts.failed, 0);
    assert!(failures_in(&archive).is_empty());

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_prune_selected_empty_calendar_event_surface_removes_target_only_events() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let _ = db::init::open(&archive).unwrap();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _event_term = anchor_terminator(&mut server, api, "CalendarEvent");
    let _event_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/query".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/query",{
                "accountId":"w","ids":["STALE"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _event_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("CalendarEvent/get".into()))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/get",{"accountId":"w","list":[{
                "id":"STALE","@type":"Event","uid":"target-only",
                "calendarIds":{"C":true},"isDraft":false,"useDefaultAlerts":false
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let destroy = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("CalendarEvent/set".into()),
            Matcher::Regex(r#""destroy":\["STALE"\]"#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["CalendarEvent/set",{"accountId":"w",
                "destroyed":["STALE"]},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let mut config = export_cfg_objects(&base, vec![ObjectType::CalendarEvent]);
    config.prune = true;
    let summary = sync::export::run(common(&archive), config).expect("prune empty surface");
    destroy.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "CalendarEvent")
        .map(|(_, counts)| counts)
        .expect("event counts");
    assert_eq!(counts.created, 0);
    assert_eq!(counts.deleted, 1);
    assert_eq!(counts.failed, 0);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_source_default_name_match_is_made_target_default() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Primary',1)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_term = anchor_terminator(&mut server, api, "Calendar");
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":["P"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _calendar_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/get".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/get",{"accountId":"w","list":[{
                "id":"P","name":"Primary","description":null,"color":null,
                "sortOrder":0,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"all","defaultAlertsWithTime":{},
                "defaultAlertsWithoutTime":{},"timeZone":null,
                "myRights":{"mayDelete":true}
            }],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let set_default = server
        .mock("POST", api)
        .match_body(Matcher::PartialJson(json!({
            "methodCalls": [["Calendar/set", {
                "accountId": "w",
                "onSuccessSetIsDefault": "P"
            }, "s"]]
        })))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{
                "accountId":"w"},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_metadata_update = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""update""#.into()),
        ]))
        .expect(0)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export");
    set_default.assert();
    no_metadata_update.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.updated, 1, "default identity changed");
    assert_eq!(counts.skipped, 0);
    assert_eq!(counts.failed, 0);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_created_default_set_failure_is_counted_and_quarantined() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,is_default) VALUES (1,'Primary',1)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _calendar_query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",{
                "accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex(r#""create":\{"c1""#.into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "created":{"c1":{"id":"NEW"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let fail_default = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessSetIsDefault".into()))
        .with_body(
            json!({"methodResponses":[["error",{
                "type":"forbidden","description":"cannot select the default"
            },"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export");
    create.assert();
    fail_default.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(ty, _)| *ty == "Calendar")
        .map(|(_, counts)| counts)
        .expect("calendar counts");
    assert_eq!(counts.created, 1);
    assert_eq!(counts.failed, 1, "default-setting failure is not silent");
    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].client_id, "c1");
    assert_eq!(rows[0].error_type, "defaultSetFailed");
    assert!(rows[0].error_detail.contains("cannot select the default"));

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_sieve_script_matches_by_name_not_content() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let keepall_local = b"require [\"fileinto\"];\nkeep;\n";
    let reject_local = b"require [\"reject\"];\nreject \"go away\";\n";
    {
        let conn = db::init::open(&archive).unwrap();
        let blob1 = db::blobs::intern_blob(&conn, keepall_local).unwrap();
        let blob2 = db::blobs::intern_blob(&conn, reject_local).unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (1,'keepall',1,?1)",
            rusqlite::params![blob1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (2,'reject',0,?1)",
            rusqlite::params![blob2],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/get",{"accountId":"w","list":[
                {"id":"S1","name":"keepall","isActive":false,"blobId":"BSRV"}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_download = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BSRV/.*".into()))
        .with_body(b"unused".as_slice())
        .expect(0)
        .create();
    let upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UPN"}).to_string())
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("reject".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c2":{"id":"S2"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _activate = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessActivateScript".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w"},"a"]]}).to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("export");
    upload.assert();
    create.assert();
    no_download.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(
        counts.skipped, 1,
        "name-matched script is skipped even though its content differs from the target"
    );
    assert_eq!(counts.created, 1, "the unmatched name is created");
    assert_eq!(counts.failed, 0);
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_sieve_scripts_identical_content_different_names_both_created() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let shared = b"require [\"fileinto\"];\nfileinto \"Archive\";\n";
    {
        let conn = db::init::open(&archive).unwrap();
        let blob = db::blobs::intern_blob(&conn, shared).unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (1,'duplicate-A',0,?1)",
            rusqlite::params![blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (2,'duplicate-B',0,?1)",
            rusqlite::params![blob],
        )
        .unwrap();
        let dup_count: i64 = conn
            .query_row(
                "SELECT count(DISTINCT blob_id) FROM sieve_scripts",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dup_count, 1, "both scripts share one blob (byte-identical)");
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/get",
                {"accountId":"w","list":[],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UPN"}).to_string())
        .expect(1)
        .create();
    let create_a = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("duplicate-A".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c1":{"id":"S1"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create_b = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("duplicate-B".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c2":{"id":"S2"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _deactivate = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessDeactivateScript".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w"},"a"]]}).to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("export");
    upload.assert();
    create_a.assert();
    create_b.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(
        counts.created, 2,
        "two scripts with identical content but distinct names must both reach the target"
    );
    assert_eq!(
        counts.skipped, 0,
        "neither distinct name collapses onto the other"
    );
    assert_eq!(counts.failed, 0);
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_sieve_blob_not_found_on_dedup_reuse_reuploads_and_retries() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let shared = b"require [\"fileinto\"];\nfileinto \"Archive\";\n";
    {
        let conn = db::init::open(&archive).unwrap();
        let blob = db::blobs::intern_blob(&conn, shared).unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (1,'dup-A',0,?1)",
            rusqlite::params![blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (2,'dup-B',0,?1)",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/get",
                {"accountId":"w","list":[],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let up1 = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(1)
        .create();
    let up2 = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP2"}).to_string())
        .expect(1)
        .create();
    let create_a = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("dup-A".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c1":{"id":"S1"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create_b_stale = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("dup-B".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "notCreated":{"c2":{"type":"blobNotFound"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create_b_fresh = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("dup-B".into()),
            Matcher::Regex("UP2".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c2":{"id":"S2"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _deactivate = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessDeactivateScript".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w"},"a"]]}).to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("export");
    up1.assert();
    up2.assert();
    create_a.assert();
    create_b_stale.assert();
    create_b_fresh.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(
        counts.created, 2,
        "the dedup-reused blobId that came back blobNotFound self-heals via re-upload"
    );
    assert_eq!(
        counts.failed, 0,
        "blobNotFound on a reused blob is not a failure"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_deeply_nested_mailbox_tree_orders_correctly() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    const DEPTH: usize = 10;

    let ids: Vec<String> = (0..DEPTH).map(|i| format!("L{i}")).collect();
    let mut servlist = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let parent = if i == 0 {
            Value::Null
        } else {
            Value::String(ids[i - 1].clone())
        };
        servlist.push(json!({
            "id": id, "name": format!("level{i}"),
            "parentId": parent, "role": null,
            "sortOrder": 0, "isSubscribed": true
        }));
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let server_ids: Vec<Value> = ids.iter().rev().map(|s| Value::String(s.clone())).collect();
    let _q = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids": server_ids},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",
                {"accountId":"w","list": servlist,"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("import");
    assert!(!summary.any_failed(), "import had failures: {summary:?}");

    let conn = rusqlite::Connection::open(&archive).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM mailboxes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n as usize, DEPTH);
    for i in 1..DEPTH {
        let pname: String = conn
            .query_row(
                "SELECT p.name FROM mailboxes c JOIN mailboxes p ON c.parent_id = p.id
                 WHERE c.name = ?1",
                rusqlite::params![format!("level{i}")],
                |r| r.get(0),
            )
            .unwrap_or_else(|e| panic!("level{i} parent lookup failed: {e}"));
        assert_eq!(pname, format!("level{}", i - 1));
    }
    let root: Option<i64> = conn
        .query_row(
            "SELECT parent_id FROM mailboxes WHERE name='level0'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(root, None, "root has no parent");
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_dry_run_sends_no_mutating_calls() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (2,'Sent',NULL,NULL)",
            [],
        )
        .unwrap();
        let blob = db::blobs::intern_blob(
            &conn,
            b"From: a@x\r\nSubject: hi\r\nMessage-ID: <m-1@h>\r\n\r\nbody",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords)
             VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]')",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();
    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let no_set = server
        .mock("POST", api)
        .match_body(Matcher::Regex(r"/(set|import)".into()))
        .expect(0)
        .create();
    let no_upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .expect(0)
        .create();

    let summary = sync::export::run(
        common_dry(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("dry-run export must succeed");
    assert!(
        !summary.any_failed(),
        "dry-run summary should not record failures: {summary:?}"
    );

    no_set.assert();
    no_upload.assert();
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_dry_run_does_not_write_archive_or_download_blobs() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    {
        let _ = db::init::open(&archive).unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");
    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["s1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let no_set = server
        .mock("POST", api)
        .match_body(Matcher::Regex(r"/(set|import)".into()))
        .expect(0)
        .create();
    let no_get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .expect(0)
        .create();
    let no_download = server
        .mock("GET", Matcher::Regex("/jmap/dl/".into()))
        .expect(0)
        .create();

    let summary = sync::import_jmap::run(
        common_dry(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("dry-run import must succeed");
    assert!(
        !summary.any_failed(),
        "dry-run import should not record failures: {summary:?}"
    );

    no_set.assert();
    no_get.assert();
    no_download.assert();

    let conn = rusqlite::Connection::open(&archive).unwrap();
    let mailbox_rows: i64 = conn
        .query_row("SELECT count(*) FROM mailboxes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mailbox_rows, 0, "dry-run must not insert into the archive");
    let source_rows: i64 = conn
        .query_row("SELECT count(*) FROM sources", [], |r| r.get(0))
        .unwrap();
    assert_eq!(source_rows, 0, "dry-run must not record the JMAP source");
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_email_keyword_change_is_propagated_without_blob_refetch() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");

    let _mbq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["MX"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _mbg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"sm1","list":[
            {"id":"MX","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let _eq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",{"accountId":"w","ids":["E1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _eg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se1","list":[
            {"id":"E1","blobId":"BLB1","receivedAt":"2020-01-01T00:00:00Z","mailboxIds":{"MX":true},"keywords":{"$seen":true}}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let _dl1 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB1/.*".into()))
        .with_body("From: a@x\r\nMessage-ID: <1@h>\r\n\r\nbody-one")
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("first import");

    let _mbq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["MX"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _mbch2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(json!({"methodResponses":[["Mailbox/changes",{"accountId":"w","oldState":"sm1","newState":"sm2","hasMoreChanges":false,"created":[],"updated":[],"destroyed":[]},"c"]]}).to_string())
        .expect(1)
        .create();
    let _eq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",{"accountId":"w","ids":["E1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let echanges = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/changes".into()))
        .with_body(json!({"methodResponses":[["Email/changes",{"accountId":"w","oldState":"se1","newState":"se2","hasMoreChanges":false,"created":[],"updated":["E1"],"destroyed":[]},"c"]]}).to_string())
        .expect(1)
        .create();
    let _eg2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se2","list":[
            {"id":"E1","mailboxIds":{"MX":true},"keywords":{"$seen":true,"$flagged":true}}
        ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let no_blob_refetch = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB1/.*".into()))
        .with_body("should-not-be-fetched")
        .expect(0)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("second import");
    echanges.assert();
    no_blob_refetch.assert();
    let em2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(em2.updated, 1, "the changed email is refreshed");
    assert_eq!(em2.fetched, 0, "no new emails");
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let kw: String = conn
        .query_row("SELECT keywords FROM emails LIMIT 1", [], |r| r.get(0))
        .unwrap();
    assert!(
        kw.contains("$seen") && kw.contains("$flagged"),
        "keyword change propagated into the archive: {kw}"
    );
    let blobs: i64 = conn
        .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        blobs, 1,
        "the immutable body blob is not re-downloaded on update"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_cannot_calculate_changes_falls_back_to_full_refresh() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["A"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s1","list":[
            {"id":"A","name":"OriginalName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("first import");

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["A"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let cannot = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(
            json!({"methodResponses":[["error",{"type":"cannotCalculateChanges"},"c"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let capture_state = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"ids\":\\[\\]".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s9","list":[],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let refresh_get = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"A\"".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s9","list":[
            {"id":"A","name":"RefreshedName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("second import");
    cannot.assert();
    capture_state.assert();
    refresh_get.assert();
    let mb2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(mb2.updated, 1, "fallback refreshes the present object");
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let name: String = conn
        .query_row("SELECT name FROM mailboxes WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "RefreshedName", "A-fallback propagated the change");
    let cursor: String = conn
        .query_row(
            "SELECT state FROM sync_state_jmap WHERE type_name='Mailbox'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        cursor, "s9",
        "fallback captured a fresh cursor for the next run"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_failed_update_holds_cursor_for_retry() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");
    let _emterm = anchor_terminator(&mut server, api, "Email");

    let _mbq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["MX"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _mbg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"sm1","list":[
            {"id":"MX","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let _eq1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",{"accountId":"w","ids":["E1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _eg1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se1","list":[
            {"id":"E1","blobId":"BLB1","receivedAt":"2020-01-01T00:00:00Z","mailboxIds":{"MX":true},"keywords":{"$seen":true}}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let _dl1 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/BLB1/.*".into()))
        .with_body("From: a@x\r\nMessage-ID: <1@h>\r\n\r\nbody-one")
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("first import");

    let _mbq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["MX"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _mbch2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(json!({"methodResponses":[["Mailbox/changes",{"accountId":"w","oldState":"sm1","newState":"sm2","hasMoreChanges":false,"created":[],"updated":[],"destroyed":[]},"c"]]}).to_string())
        .expect(1)
        .create();
    let _eq2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",{"accountId":"w","ids":["E1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _ech2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/changes".into()))
        .with_body(json!({"methodResponses":[["Email/changes",{"accountId":"w","oldState":"se1","newState":"se2","hasMoreChanges":false,"created":[],"updated":["E1"],"destroyed":[]},"c"]]}).to_string())
        .expect(1)
        .create();
    let bad_update = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","state":"se2","list":[
            {"id":"E1","mailboxIds":{},"keywords":{"$seen":true,"$flagged":true}}
        ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("second import");
    bad_update.assert();
    let em2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts");
    assert_eq!(
        em2.updated, 0,
        "the failed update is not counted as applied"
    );
    assert!(em2.failed >= 1, "the unresolvable update is counted failed");
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let cursor: String = conn
        .query_row(
            "SELECT state FROM sync_state_jmap WHERE type_name='Email'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        cursor, "se1",
        "cursor is held at the pre-change state so the failed update retries next run"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_unknown_method_changes_falls_back_to_full_refresh() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["A"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s1","list":[
            {"id":"A","name":"OriginalName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("first import");

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["A"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let unknown = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/changes".into()))
        .with_body(json!({"methodResponses":[["error",{"type":"unknownMethod"},"c"]]}).to_string())
        .expect(1)
        .create();
    let _capture_state = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"ids\":\\[\\]".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s9","list":[],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    let refresh_get = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"A\"".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s9","list":[
            {"id":"A","name":"RefreshedName","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("second import");
    unknown.assert();
    refresh_get.assert();
    let mb2 = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(
        mb2.updated, 1,
        "a server without Mailbox/changes degrades to a full refresh instead of aborting"
    );
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let name: String = conn
        .query_row("SELECT name FROM mailboxes WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "RefreshedName");
    let _ = std::fs::remove_file(&archive);
}

fn sieve_get_body(name: &str, blob: &str) -> String {
    json!({"methodResponses":[["SieveScript/get",{"accountId":"w","state":"x","list":[
        {"id":"S1","name":name,"isActive":true,"blobId":blob}
    ],"notFound":[]},"g"]]})
    .to_string()
}

#[test]
fn import_sieve_script_reimport_unchanged_is_convergent() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "SieveScript");
    let _dl = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/B1/.*".into()))
        .with_body("keep;\n")
        .create();

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/query".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/query",{"accountId":"w","ids":["S1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(sieve_get_body("main", "B1"))
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("first import");

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/query".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/query",{"accountId":"w","ids":["S1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(sieve_get_body("main", "B1"))
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("second import");
    let ss = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(ss.created, 0, "no new scripts");
    assert_eq!(
        ss.updated, 0,
        "an unchanged SieveScript must not be counted as updated on re-import (convergent)"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_sieve_script_content_change_is_propagated() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "SieveScript");
    let _dl1 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/B1/.*".into()))
        .with_body("keep;\n")
        .create();
    let _dl2 = server
        .mock("GET", Matcher::Regex("/jmap/dl/w/B2/.*".into()))
        .with_body("discard;\n")
        .create();

    let _q1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/query".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/query",{"accountId":"w","ids":["S1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g1 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(sieve_get_body("main", "B1"))
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("first import");

    let _q2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/query".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/query",{"accountId":"w","ids":["S1"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    let _g2 = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(sieve_get_body("main", "B2"))
        .expect(1)
        .create();

    let s2 = sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("second import");
    let ss = s2
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(
        ss.updated, 1,
        "the changed script content is re-fetched and updated"
    );
    let conn = rusqlite::Connection::open(&archive).unwrap();
    let body: Vec<u8> = conn
        .query_row(
            "SELECT b.data FROM blobs b JOIN sieve_scripts s ON s.blob_id = b.id LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        String::from_utf8(body).unwrap(),
        "discard;\n",
        "new script content propagated into the archive blob"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn first_run_cursor_is_captured_up_front_not_from_the_fetch() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");
    let _q = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",{"accountId":"w","ids":["A"]},"q"]]})
                .to_string(),
        )
        .expect(1)
        .create();
    // Up-front state snapshot (ids:[]) reports an EARLIER state than the new-fetch.
    let _state = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"ids\":\\[\\]".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"before","list":[],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();
    // The new-fetch reports a LATER state; if we (incorrectly) captured from here, the cursor
    // would be "after" and an object changed mid-run could be missed next run.
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/get".into()),
            Matcher::Regex("\"A\"".into()),
        ]))
        .with_body(json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"after","list":[
            {"id":"A","name":"Personal","parentId":null,"role":null,"sortOrder":0,"isSubscribed":true}
        ],"notFound":[]},"g"]]}).to_string())
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("import");
    let cursor: String = rusqlite::Connection::open(&archive)
        .unwrap()
        .query_row(
            "SELECT state FROM sync_state_jmap WHERE type_name='Mailbox'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        cursor, "before",
        "cursor must be the pre-fetch snapshot (lower bound), not the post-fetch state"
    );
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_duplicate_role_mailbox_created_as_plain_folder_keeping_subtree() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (2,'Sent',NULL,'sent')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (3,'Éléments envoyés',NULL,'sent')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (4,'Brouillons locaux',3,NULL)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _mbterm = anchor_terminator(&mut server, api, "Mailbox");

    let _mq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["TI","TS"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _mg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
                {"id":"TI","name":"Inbox","role":"inbox","parentId":null,"myRights":{"mayDelete":true}},
                {"id":"TS","name":"Sent","role":"sent","parentId":null,"myRights":{"mayDelete":true}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let role_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/set".into()),
            Matcher::Regex("envoy".into()),
            Matcher::Regex("\"role\"".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Mailbox/set",{"accountId":"w",
                "notCreated":{"c3":{"type":"invalidProperties","properties":["role"],
                "description":"A mailbox with role 'sent' already exists."}}},"s"]]})
            .to_string(),
        )
        .expect_at_most(1)
        .create();
    let folder_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/set".into()),
            Matcher::Regex("envoy".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Mailbox/set",{"accountId":"w",
                "created":{"c3":{"id":"M3"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let child_create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Mailbox/set".into()),
            Matcher::Regex("Brouillons".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Mailbox/set",{"accountId":"w",
                "created":{"c4":{"id":"M4"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("export");

    role_create.assert();
    folder_create.assert();
    child_create.assert();

    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(
        counts.skipped, 2,
        "Inbox and Sent match existing role mailboxes"
    );
    assert_eq!(
        counts.created, 2,
        "duplicate-role folder and its child are both created"
    );
    assert_eq!(counts.failed, 0, "no cascade skip of the subtree");
    let _ = std::fs::remove_file(&archive);
}

#[test]
fn import_jmap_duplicate_role_is_deduplicated_to_single_mailbox() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");

    let _q = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["A","B","C"]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","state":"s1","list":[
                {"id":"A","name":"Sent","parentId":null,"role":"sent","sortOrder":0,"isSubscribed":true},
                {"id":"B","name":"Éléments envoyés","parentId":null,"role":"sent","sortOrder":0,"isSubscribed":true},
                {"id":"C","name":"Sent Items","parentId":null,"role":"sent","sortOrder":0,"isSubscribed":true}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    sync::import_jmap::run(
        common(&archive),
        import_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("import");

    let conn = rusqlite::Connection::open(&archive).unwrap();
    let total: i64 = conn
        .query_row("SELECT count(*) FROM mailboxes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 3, "all three folders are imported");
    let with_role: i64 = conn
        .query_row(
            "SELECT count(*) FROM mailboxes WHERE role = 'sent'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(with_role, 1, "exactly one mailbox keeps the sent role");
    let null_roles: i64 = conn
        .query_row(
            "SELECT count(*) FROM mailboxes WHERE role IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(null_roles, 2, "the surplus duplicates become plain folders");
    drop(conn);
    let _ = std::fs::remove_file(&archive);
}

// ---------------------------------------------------------------------------
// Per-item export failures.
//
// Aggregate counters (`Email: ... failed=N`) tell a caller how many items the
// target refused but not which ones, so every failing path now also writes a
// structured row into the archive. The blobNotFound cases below model a
// CONTENT-ADDRESSED target: byte-identical uploads come back with the SAME
// blobId, which is what makes a re-upload incapable of clearing a de-duplication
// marker whose bytes have gone missing from the blob store.
// ---------------------------------------------------------------------------

fn session_body_blob(base: &str) -> String {
    json!({
        "apiUrl": format!("{base}/jmap/api"),
        "uploadUrl": format!("{base}/jmap/upload/{{accountId}}/"),
        "downloadUrl": format!("{base}/jmap/dl/{{accountId}}/{{blobId}}/{{type}}/{{name}}"),
        "capabilities": { "urn:ietf:params:jmap:core": {
            "maxObjectsInGet": 500, "maxObjectsInSet": 500, "maxCallsInRequest": 16,
            "maxConcurrentRequests": 4, "maxConcurrentUpload": 4,
            "maxSizeRequest": 10000000, "maxSizeUpload": 50000000
        } },
        "accounts": { "w": { "name": "alice",
            "accountCapabilities": {
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:blob": {}
            } } }
    })
    .to_string()
}

fn seed_one_email(archive: &Path, mid: &str) -> Vec<u8> {
    let conn = db::init::open(archive).unwrap();
    conn.execute(
        "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Inbox',NULL,'inbox')",
        [],
    )
    .unwrap();
    let raw = format!(
        "From: alex@example.test\r\nSubject: quarantine\r\nMessage-ID: <{mid}>\r\n\r\nbody"
    )
    .into_bytes();
    let blob = db::blobs::intern_blob(&conn, &raw).unwrap();
    conn.execute(
        "INSERT INTO emails (blob_id,received_at,mailbox_ids,keywords,message_match)
         VALUES (?1,'2020-01-01T00:00:00Z','[1]','[]',?2)",
        rusqlite::params![blob, json!({ "m": [mid], "f": "0".repeat(64) }).to_string()],
    )
    .unwrap();
    raw
}

/// Mailbox side of an email export: one mailbox already on the target, mapped
/// by name, so the run gets straight to the message.
fn export_mailbox_scaffold(server: &mut mockito::Server, api: &str) -> Vec<mockito::Mock> {
    let term = anchor_terminator(server, api, "Mailbox");
    let query = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
            {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let get = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
            {"id":"t1","name":"Inbox","role":"inbox","parentId":null,
             "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    vec![term, query, get]
}

fn empty_email_query(server: &mut mockito::Server, api: &str) -> mockito::Mock {
    server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create()
}

fn failures_in(archive: &Path) -> Vec<db::export_failures::FailedItem> {
    let conn = db::init::open(archive).unwrap();
    db::export_failures::list(&conn, None, 0).unwrap()
}

fn email_counts(summary: &sync::Summary) -> sync::TypeCounts {
    summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Email")
        .map(|(_, c)| c.clone())
        .expect("email counts")
}

#[test]
fn export_email_failure_is_recorded_as_a_structured_item() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    let raw = seed_one_email(&archive, "q-1@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(1)
        .create();
    let imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"invalidProperties",
                                    "description":"receivedAt is not a UTCDate"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    let email = email_counts(&summary);
    assert_eq!(email.failed, 1, "the refused message counts as failed");
    assert_eq!(email.created, 0);
    imp.assert();

    let rows = failures_in(&archive);
    assert_eq!(
        rows.len(),
        1,
        "one quarantine row per refused item: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row.type_name, "email");
    assert_eq!(row.local_id, 1);
    assert_eq!(row.client_id, "e1", "the creation id used on the wire");
    assert_eq!(row.message_id.as_deref(), Some("q-1@h"));
    assert_eq!(row.size_bytes, Some(raw.len() as i64));
    assert_eq!(row.target_blob_id.as_deref(), Some("UP1"));
    assert_eq!(row.error_type, "invalidProperties");
    assert!(
        row.error_detail.contains("receivedAt is not a UTCDate"),
        "the full target detail is kept: {}",
        row.error_detail
    );
    assert_eq!(
        row.blob_probe, "not_probed",
        "no blob probe runs for a failure that is not blobNotFound"
    );
    assert_eq!(
        row.blob_hash.as_deref(),
        Some(blake3::hash(&raw).to_hex().to_string().as_str()),
        "the full content hash correlates the item with the target's dedup key"
    );
    assert_eq!(
        row.blob_local_id,
        Some(1),
        "the bytes stay reachable in the archive"
    );
    assert!(row.failed_at.ends_with('Z'), "got {}", row.failed_at);

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_failure_row_is_replaced_not_duplicated_on_rerun() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-2@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let first = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"invalidProperties"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let second = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"overQuota"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    for pass in 1..=2 {
        let summary = sync::export::run(
            common(&archive),
            export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
        )
        .unwrap_or_else(|e| panic!("export pass {pass}: {e}"));
        assert_eq!(email_counts(&summary).failed, 1, "pass {pass}");
    }
    first.assert();
    second.assert();

    let rows = failures_in(&archive);
    assert_eq!(
        rows.len(),
        1,
        "a re-run overwrites the row instead of appending: {rows:?}"
    );
    assert_eq!(
        rows[0].error_type, "overQuota",
        "the row reflects the latest attempt, not the first"
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blob_not_found_on_content_addressed_target_records_orphaned_marker() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-3@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_blob(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    // Identical bytes hash the same, so a content-addressed target hands back
    // the same blobId for both uploads: the re-upload is de-duplicated away.
    let up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"blobNotFound"}}},"i"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let probe = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Blob/get".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Blob/get",
            {"accountId":"w","list":[],"notFound":["UP1"]},"b"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    assert_eq!(
        email_counts(&summary).failed,
        1,
        "a blobNotFound that survives the re-upload is a real failure, not a self-heal"
    );
    assert!(summary.any_failed());
    up.assert();
    imp.assert();
    probe.assert();

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(rows[0].error_type, "blobNotFound");
    assert_eq!(rows[0].target_blob_id.as_deref(), Some("UP1"));
    assert_eq!(
        rows[0].blob_probe, "orphaned_marker",
        "upload 200 plus Blob/get notFound is the orphaned dedup marker signature"
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blob_probe_failure_is_recorded_as_store_unavailable_not_orphaned() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-4@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_blob(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let _imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"blobNotFound"}}},"i"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let probe = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Blob/get".into()))
        .with_status(500)
        .with_body("blob store unreachable")
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    assert_eq!(email_counts(&summary).failed, 1);
    probe.assert();

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(
        rows[0].blob_probe, "store_unavailable",
        "a probe that cannot answer must not be read as proof of an orphan"
    );
    assert!(
        rows[0].error_detail.contains("blob probe"),
        "the probe failure is kept verbatim: {}",
        rows[0].error_detail
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blob_probe_reads_a_size_only_result_as_retrievable() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-4b@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_blob(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let _imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"blobNotFound"}}},"i"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    // The probe asks for `properties: ["size"]`. RFC 8620 5.1 says `id` comes
    // back regardless, but a target that takes the property list literally
    // answers with the size alone — and in doing so proves it read the bytes out
    // of its blob store. Only one id was asked about, so the lone entry can only
    // be about it.
    let probe = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Blob/get".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Blob/get",
            {"accountId":"w","list":[{"size":64}],"notFound":[]},"b"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    assert_eq!(email_counts(&summary).failed, 1);
    probe.assert();

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(
        rows[0].blob_probe, "retrievable",
        "a target that answered with the blob's size is serving that blob, not \
         an unhealthy blob store"
    );
    assert!(
        rows[0].error_detail.contains("64 bytes"),
        "the size the target reported is kept: {}",
        rows[0].error_detail
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_blob_probe_is_unsupported_when_target_lacks_the_blob_capability() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-5@h");

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let _imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"blobNotFound"}}},"i"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let probe = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Blob/get".into()))
        .with_body(json!({"methodResponses":[["Blob/get",{"accountId":"w"},"b"]]}).to_string())
        .expect(0)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    assert_eq!(email_counts(&summary).failed, 1);
    probe.assert();

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(
        rows[0].blob_probe, "unsupported",
        "retrievability is reported as unknown rather than guessed at"
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_email_quarantine_row_is_cleared_once_the_item_is_on_target() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-6@h");
    {
        let conn = db::init::open(&archive).unwrap();
        db::export_failures::record(
            &conn,
            &db::export_failures::FailedItem {
                type_name: "email".to_owned(),
                local_id: 1,
                client_id: "e1".to_owned(),
                message_id: Some("q-6@h".to_owned()),
                size_bytes: Some(64),
                blob_local_id: Some(1),
                blob_hash: None,
                target_blob_id: Some("UP1".to_owned()),
                error_type: "blobNotFound".to_owned(),
                error_detail: "left over from an earlier run".to_owned(),
                blob_probe: db::export_failures::PROBE_ORPHANED_MARKER.to_owned(),
                failed_at: "2026-01-01T00:00:00Z".to_owned(),
            },
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _emterm = anchor_terminator(&mut server, api, "Email");
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":["X1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _eg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","list":[
            {"id":"X1","messageId":["q-6@h"]}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    let email = email_counts(&summary);
    assert_eq!(email.skipped, 1, "the message is already on the target");
    assert_eq!(email.failed, 0);

    assert!(
        failures_in(&archive).is_empty(),
        "a resolved item must not stay quarantined"
    );

    let _ = std::fs::remove_file(&archive);
}

/// A row left behind by an earlier run, of whatever type.
fn stale_row(type_name: &str, local_id: i64) -> db::export_failures::FailedItem {
    db::export_failures::FailedItem {
        type_name: type_name.to_owned(),
        local_id,
        client_id: format!("x{local_id}"),
        message_id: None,
        size_bytes: Some(64),
        blob_local_id: Some(1),
        blob_hash: None,
        target_blob_id: Some("UP1".to_owned()),
        error_type: "blobNotFound".to_owned(),
        error_detail: "recorded before the archive was re-imported".to_owned(),
        blob_probe: db::export_failures::PROBE_ORPHANED_MARKER.to_owned(),
        failed_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

#[test]
fn export_reaps_quarantine_rows_whose_archive_object_is_gone() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-8@h");
    {
        let conn = db::init::open(&archive).unwrap();
        // Model the aftermath of a UIDVALIDITY change: the coordinator wipes the
        // folder's `emails` rows and the re-import inserts the same messages
        // again under fresh local ids. The row an earlier export left for the OLD
        // id now names nothing in the archive, and the reconcile loop below only
        // ever visits the NEW one, so `resolve()` can never reach it.
        db::export_failures::record(&conn, &stale_row("email", 1)).unwrap();
        conn.execute("UPDATE emails SET id = 2 WHERE id = 1", [])
            .unwrap();
        // A type with no rows at all is dropped from the work list by `has_rows`,
        // which is precisely the case where every row it has is stale — so the
        // sweep cannot be hung off the per-type pass and has to cover all types.
        db::export_failures::record(&conn, &stale_row("sievescript", 7)).unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _emterm = anchor_terminator(&mut server, api, "Email");
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":["X1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _eg = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/get".into()))
        .with_body(
            json!({"methodResponses":[["Email/get",{"accountId":"w","list":[
            {"id":"X1","messageId":["q-8@h"]}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    let email = email_counts(&summary);
    assert_eq!(
        email.skipped, 1,
        "the re-imported message is matched under its new local id"
    );
    assert_eq!(email.failed, 0);

    assert!(
        failures_in(&archive).is_empty(),
        "a row naming an object that is no longer in the archive is a phantom \
         failure and pins the message bytes against gc; it must not survive the run"
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_dry_run_records_no_quarantine_rows() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-7@h");
    {
        // No mailbox mapping is possible in a dry run against an empty target,
        // so this item takes a failing path; the archive must still be untouched.
        let conn = db::init::open(&archive).unwrap();
        conn.execute("DELETE FROM mailboxes", []).unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _emterm = anchor_terminator(&mut server, api, "Email");
    let _eq = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/query".into()))
        .with_body(
            json!({"methodResponses":[["Email/query",
            {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    sync::export::run(
        common_dry(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("dry-run export");

    assert!(
        failures_in(&archive).is_empty(),
        "--dry-run must stay side-effect free"
    );

    let _ = std::fs::remove_file(&archive);
}

#[test]
fn export_quarantine_write_failure_is_not_counted_as_a_refused_object() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";

    let archive = tmp();
    seed_one_email(&archive, "q-8@h");
    {
        // Stand in for the archive refusing the write (disk full, SQLITE_BUSY):
        // the item still fails on the target, but its row cannot be laid down.
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "CREATE TRIGGER refuse_quarantine_writes BEFORE INSERT ON export_failures
             BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _scaffold = export_mailbox_scaffold(&mut server, api);
    let _eq = empty_email_query(&mut server, api);
    let _up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(1)
        .create();
    let _imp = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Email/import".into()))
        .with_body(
            json!({"methodResponses":[["Email/import",{"accountId":"w",
                "notCreated":{"e1":{"type":"invalidProperties",
                                    "description":"receivedAt is not a UTCDate"}}},"i"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox, ObjectType::Email]),
    )
    .expect("export run");

    assert_eq!(
        email_counts(&summary).failed,
        1,
        "one message was refused, so `failed` is 1: a bookkeeping error must not \
         add a unit that corresponds to no item, or a consumer reconciling \
         failed=N against the rows finds a discrepancy it cannot attribute"
    );
    assert_eq!(
        summary.unrecorded_failures.len(),
        1,
        "the lost row is still surfaced, on its own channel: {:?}",
        summary.unrecorded_failures
    );
    assert!(
        summary.unrecorded_failures[0].contains("Email")
            && summary.unrecorded_failures[0].contains("disk I/O error"),
        "the signal names the type and the write error: {}",
        summary.unrecorded_failures[0]
    );

    assert!(
        failures_in(&archive).is_empty(),
        "the write was refused, so there is no row"
    );

    let _ = std::fs::remove_file(&archive);
}

/// Sieve is exported through the same content-addressed blob endpoint as mail,
/// and both the sieve and the blob capability have to be on for the probe to
/// run, so neither `session_body_full` nor `session_body_blob` alone will do.
fn session_body_sieve_blob(base: &str) -> String {
    json!({
        "apiUrl": format!("{base}/jmap/api"),
        "uploadUrl": format!("{base}/jmap/upload/{{accountId}}/"),
        "downloadUrl": format!("{base}/jmap/dl/{{accountId}}/{{blobId}}/{{type}}/{{name}}"),
        "capabilities": { "urn:ietf:params:jmap:core": {
            "maxObjectsInGet": 500, "maxObjectsInSet": 500, "maxCallsInRequest": 16,
            "maxConcurrentRequests": 4, "maxConcurrentUpload": 4,
            "maxSizeRequest": 10000000, "maxSizeUpload": 50000000
        } },
        "accounts": { "w": { "name": "alice",
            "accountCapabilities": {
                "urn:ietf:params:jmap:sieve": {},
                "urn:ietf:params:jmap:blob": {}
            } } }
    })
    .to_string()
}

/// The quarantine has to cover the whole archive, not just mail.
///
/// A Sieve script rides the very same de-duplicating upload path a message does,
/// so an orphaned marker can shadow one exactly the same way — and while only the
/// Email reconciler filed rows, this run printed `SieveScript: ... failed=1` and
/// exited 5 while `inspect --failures` still reported `Export failures (0)`. A
/// caller that reads "no rows" as "nothing was refused" called that clean.
#[test]
fn export_sieve_blob_not_found_surviving_reupload_is_quarantined_as_orphaned_marker() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let script = b"require [\"fileinto\"];\nfileinto \"Archive\";\n";
    {
        let conn = db::init::open(&archive).unwrap();
        let blob = db::blobs::intern_blob(&conn, script).unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (1,'orphaned',0,?1)",
            rusqlite::params![blob],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_sieve_blob(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/get",
                {"accountId":"w","list":[],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    // Identical bytes hash the same, so the re-upload is de-duplicated onto the
    // same marker and comes back as the same blobId: the retry cannot help.
    let up = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .with_body(json!({"blobId":"UP1"}).to_string())
        .expect(2)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "notCreated":{"c1":{"type":"blobNotFound"}}},"s"]]})
            .to_string(),
        )
        .expect(2)
        .create();
    let probe = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Blob/get".into()),
            Matcher::Regex("UP1".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Blob/get",
                {"accountId":"w","list":[],"notFound":["UP1"]},"b"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _deactivate = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessDeactivateScript".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w"},"a"]]}).to_string(),
        )
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("export run");

    up.assert();
    create.assert();
    probe.assert();
    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts");
    assert_eq!(counts.failed, 1, "the refusal survived the re-upload");
    assert!(summary.any_failed());

    let rows = failures_in(&archive);
    assert_eq!(
        rows.len(),
        1,
        "a failed=1 the caller cannot resolve to an item is the whole bug: {rows:?}"
    );
    assert_eq!(rows[0].type_name, "sievescript");
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].client_id, "c1");
    assert_eq!(rows[0].error_type, "blobNotFound");
    assert_eq!(rows[0].target_blob_id.as_deref(), Some("UP1"));
    assert_eq!(
        rows[0].blob_probe, "orphaned_marker",
        "upload 200 plus Blob/get notFound is the orphaned dedup marker signature"
    );
    assert_eq!(
        rows[0].size_bytes,
        Some(script.len() as i64),
        "the script's own bytes identify it"
    );
    assert_eq!(
        rows[0].blob_hash.as_deref().map(str::len),
        Some(64),
        "the blake3 is the key the target de-duplicates on: {rows:?}"
    );

    let _ = std::fs::remove_file(&archive);
}

/// A type with no blob at all still has to leave a trace, or the table means
/// "refused Email and blob-backed objects" rather than "refused".
#[test]
fn export_address_book_refusal_is_quarantined_and_cleared_once_it_lands() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO address_books (id,name,description,is_default)
             VALUES (1,'Work',NULL,0)",
            [],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("AddressBook/get".into()))
        .with_body(
            json!({"methodResponses":[["AddressBook/get",
                {"accountId":"w","list":[],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let refuse = server
        .mock("POST", api)
        .match_body(Matcher::Regex("AddressBook/set".into()))
        .with_body(
            json!({"methodResponses":[["AddressBook/set",{"accountId":"w",
                "notCreated":{"c1":{"type":"forbidden","description":"quota exceeded"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::AddressBook]),
    )
    .expect("export run");
    refuse.assert();
    assert!(summary.any_failed());

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(rows[0].type_name, "addressbook");
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].error_type, "forbidden");
    assert!(
        rows[0].error_detail.contains("quota exceeded"),
        "the target's own words are what an operator acts on: {rows:?}"
    );
    assert_eq!(
        rows[0].blob_probe, "not_probed",
        "nothing here goes through the blob store, so nothing was probed"
    );
    assert_eq!(rows[0].blob_local_id, None);

    // Second run: the book is on the target, so the row describes a failure that
    // no longer holds and must not keep reporting itself as unresolved.
    let mut server2 = mockito::Server::new();
    let base2 = server2.url();
    let _root2 = server2.mock("GET", "/").with_status(404).create();
    let _wk2 = server2
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base2))
        .expect_at_least(1)
        .create();
    let _g2 = server2
        .mock("POST", api)
        .match_body(Matcher::Regex("AddressBook/get".into()))
        .with_body(
            json!({"methodResponses":[["AddressBook/get",{"accountId":"w","list":[
                {"id":"WID","name":"Work","isDefault":false,"myRights":{"mayDelete":true}}
            ],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();

    let summary2 = sync::export::run(
        common(&archive),
        export_cfg_objects(&base2, vec![ObjectType::AddressBook]),
    )
    .expect("second export run");
    assert!(!summary2.any_failed());
    assert!(
        failures_in(&archive).is_empty(),
        "a resolved item must not stay quarantined"
    );

    let _ = std::fs::remove_file(&archive);
}

fn sieve_counts(summary: &sync::Summary) -> sync::TypeCounts {
    summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "SieveScript")
        .map(|(_, c)| c.clone())
        .expect("sieve counts")
}

/// One script that will not go up must not take the rest of the surface with it.
///
/// The upload is the only fallible step in building a SieveScript create, and it
/// used to leave the reconciler by `?`. That ended the WHOLE pass: `run()` caught
/// it as `type SieveScript aborted`, charged the abort a single `failed` unit —
/// with no row behind it, the blind spot the quarantine exists to close — and
/// every script queued behind the broken one went unattempted while the count
/// still said one. A caller reconciling `failed=N` against the archive would
/// find neither the script that broke nor the ones that were never tried.
#[test]
fn export_sieve_upload_failure_quarantines_one_script_without_aborting_the_surface() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    let broken = b"require [\"fileinto\"];\nfileinto \"Archive\";\n";
    let sound = b"keep;\n";
    {
        let conn = db::init::open(&archive).unwrap();
        let b1 = db::blobs::intern_blob(&conn, broken).unwrap();
        let b2 = db::blobs::intern_blob(&conn, sound).unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (1,'refused',0,?1)",
            rusqlite::params![b1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sieve_scripts (id,name,is_active,blob_id) VALUES (2,'accepted',0,?1)",
            rusqlite::params![b2],
        )
        .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_sieve_blob(&base))
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("SieveScript/get".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/get",
                {"accountId":"w","list":[],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    // A 200 that names no blobId: the target took the bytes and told us nothing
    // we can put in a create. Only the first script's upload answers this way.
    let bad_upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .match_body(Matcher::Regex("fileinto".into()))
        .with_body(json!({ "stored": true }).to_string())
        .expect(1)
        .create();
    let good_upload = server
        .mock("POST", Matcher::Regex("/jmap/upload/".into()))
        .match_body(Matcher::Regex("keep;".into()))
        .with_body(json!({ "blobId": "UP2" }).to_string())
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("SieveScript/set".into()),
            Matcher::Regex("UP2".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w",
                "created":{"c2":{"id":"S2"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let _deactivate = server
        .mock("POST", api)
        .match_body(Matcher::Regex("onSuccessDeactivateScript".into()))
        .with_body(
            json!({"methodResponses":[["SieveScript/set",{"accountId":"w"},"a"]]}).to_string(),
        )
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::SieveScript]),
    )
    .expect("export run");

    bad_upload.assert();
    good_upload.assert();
    create.assert();

    let counts = sieve_counts(&summary);
    assert_eq!(counts.failed, 1, "one script, not one aborted surface");
    assert_eq!(
        counts.created, 1,
        "the script queued behind the broken one still has to be attempted"
    );
    assert!(summary.any_failed());

    let rows = failures_in(&archive);
    assert_eq!(
        rows.len(),
        1,
        "a failed=1 the caller cannot resolve to an item is the whole bug: {rows:?}"
    );
    assert_eq!(rows[0].type_name, "sievescript");
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].client_id, "c1");
    assert_eq!(
        rows[0].error_type, "blobUploadFailed",
        "the same category the Email path records for bytes that would not go up"
    );
    assert!(
        rows[0].error_detail.contains("blobId"),
        "the row carries what the target actually did: {rows:?}"
    );
    assert_eq!(
        rows[0].size_bytes,
        Some(broken.len() as i64),
        "the script's own bytes identify it even though the upload failed"
    );
    assert_eq!(
        rows[0].blob_hash.as_deref().map(str::len),
        Some(64),
        "the blake3 is what correlates the item with the target's store: {rows:?}"
    );
    assert_eq!(
        rows[0].blob_probe, "not_probed",
        "nothing was uploaded, so there is no blobId to probe"
    );

    let _ = std::fs::remove_file(&archive);
}

/// A calendar that will not build is one item, not the whole calendar surface.
///
/// `default_alerts_with_time` is CHECKed for well-formed JSON but not for shape,
/// so an archive can legitimately hold a value that passes SQLite and then fails
/// to deserialize. That used to leave the reconciler by `?` and abort the type:
/// every other calendar went uncreated and the abort was charged one `failed`
/// unit with nothing in the quarantine to say which row was at fault.
#[test]
fn export_calendar_that_will_not_build_is_quarantined_without_aborting_the_surface() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        conn.execute(
            "INSERT INTO calendars (id,name,default_alerts_with_time) VALUES (1,'Broken','[1,2,3]')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO calendars (id,name) VALUES (2,'Team')", [])
            .unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body_full(&base))
        .expect_at_least(1)
        .create();
    let _q = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Calendar/query".into()))
        .with_body(
            json!({"methodResponses":[["Calendar/query",
                {"accountId":"w","ids":[]},"q"]]})
            .to_string(),
        )
        .expect(1)
        .create();
    let create = server
        .mock("POST", api)
        .match_body(Matcher::AllOf(vec![
            Matcher::Regex("Calendar/set".into()),
            Matcher::Regex("Team".into()),
        ]))
        .with_body(
            json!({"methodResponses":[["Calendar/set",{"accountId":"w",
                "created":{"c2":{"id":"CID2"}}},"s"]]})
            .to_string(),
        )
        .expect(1)
        .create();

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Calendar]),
    )
    .expect("export run");
    create.assert();

    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Calendar")
        .map(|(_, c)| c.clone())
        .expect("calendar counts");
    assert_eq!(counts.failed, 1, "one calendar, not one aborted surface");
    assert_eq!(
        counts.created, 1,
        "the sound calendar still has to reach the target"
    );

    let rows = failures_in(&archive);
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(rows[0].type_name, "calendar");
    assert_eq!(rows[0].local_id, 1);
    assert_eq!(rows[0].client_id, "c1");
    assert_eq!(rows[0].error_type, "buildFailed");
    assert_eq!(
        rows[0].blob_local_id, None,
        "a calendar carries no blob, so there is none to name"
    );

    let _ = std::fs::remove_file(&archive);
}

/// Merging onto a target object that is already there IS landing on the target,
/// so the row a previous run left behind has to go with it.
///
/// This is the one landing `reap_missing` can never clean up after: the archive
/// object is still present under the same local id, so the sweep correctly keeps
/// the row, and the merge short-circuits before any `/set` call, so neither the
/// `created` nor the `alreadyExists` branch is reached either. Left unresolved
/// the row is a permanent phantom — `inspect --failures` keeps naming a mailbox
/// that migrated fine, and a caller gating on "rows present == still unresolved"
/// can never be cleared.
#[test]
fn export_mailbox_merged_onto_a_name_collision_clears_its_quarantine_row() {
    let mut server = mockito::Server::new();
    let base = server.url();
    let api = "/jmap/api";
    let archive = tmp();
    {
        let conn = db::init::open(&archive).unwrap();
        // Two archive folders folding to the same name: the first is matched by
        // the name pre-pass and claims the target, so the second can only reach
        // it through `find_name_collision`.
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (1,'Work',NULL,NULL)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mailboxes (id,name,parent_id,role) VALUES (2,'work',NULL,NULL)",
            [],
        )
        .unwrap();
        let mut row = stale_row("mailbox", 2);
        row.error_type = "forbidden".to_owned();
        row.error_detail = "refused by an earlier run".to_owned();
        row.blob_local_id = None;
        row.blob_probe = db::export_failures::PROBE_NOT_PROBED.to_owned();
        db::export_failures::record(&conn, &row).unwrap();
    }

    let _root = server.mock("GET", "/").with_status(404).create();
    let _wk = server
        .mock("GET", "/.well-known/jmap")
        .with_body(session_body(&base))
        .expect_at_least(1)
        .create();
    let _term = anchor_terminator(&mut server, api, "Mailbox");
    let _q = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/query".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/query",
                {"accountId":"w","ids":["t1"]},"q"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    let _g = server
        .mock("POST", api)
        .match_body(Matcher::Regex("Mailbox/get".into()))
        .with_body(
            json!({"methodResponses":[["Mailbox/get",{"accountId":"w","list":[
                {"id":"t1","name":"Work","role":null,"parentId":null,
                 "myRights":{"mayDelete":true}}],"notFound":[]},"g"]]})
            .to_string(),
        )
        .expect_at_least(1)
        .create();
    // No Mailbox/set is registered on purpose: both locals resolve to the target
    // that is already there, so a create attempt would fail the run outright.

    let summary = sync::export::run(
        common(&archive),
        export_cfg_objects(&base, vec![ObjectType::Mailbox]),
    )
    .expect("export run");

    let counts = summary
        .per_type
        .iter()
        .find(|(t, _)| *t == "Mailbox")
        .map(|(_, c)| c.clone())
        .expect("mailbox counts");
    assert_eq!(counts.created, 0);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.skipped, 2, "one matched, one merged");
    assert!(!summary.any_failed());

    assert!(
        failures_in(&archive).is_empty(),
        "the merged folder is on the target; a row still naming it is a phantom \
         failure nothing else in the lifecycle can ever clear"
    );

    let _ = std::fs::remove_file(&archive);
}
