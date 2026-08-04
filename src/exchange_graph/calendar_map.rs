/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use serde_json::{Map, Value, json};

use crate::exchange::tz::resolve_to_iana;
use crate::exchange_graph::error::GraphError;
use crate::exchange_graph::recurrence::convert_patterned_recurrence_rule;

pub fn graph_calendar_color_to_hex(value: &str) -> Option<&'static str> {
    match value {
        "lightBlue" => Some("#A6CEE3"),
        "lightGreen" => Some("#B2DF8A"),
        "lightOrange" => Some("#FDBF6F"),
        "lightGray" => Some("#CCCCCC"),
        "lightYellow" => Some("#FFFF99"),
        "lightTeal" => Some("#A0E7E5"),
        "lightPink" => Some("#FB9A99"),
        "lightBrown" => Some("#B15928"),
        "lightRed" => Some("#FB9A99"),
        _ => None,
    }
}

pub fn windows_or_iana_to_iana(value: &str) -> Option<String> {
    resolve_to_iana(value)
}

fn utc_naive_to_local(utc_naive: &str, iana: &str) -> Option<String> {
    use chrono::{NaiveDateTime, TimeZone};
    use chrono_tz::Tz;

    let tz: Tz = iana.parse().ok()?;
    let naive = NaiveDateTime::parse_from_str(utc_naive, "%Y-%m-%dT%H:%M:%S").ok()?;
    Some(
        tz.from_utc_datetime(&naive)
            .naive_local()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string(),
    )
}

#[derive(Debug, Clone)]
pub struct ConvertedEvent {
    pub uid: String,
    pub data: Value,
    pub is_draft: bool,
    pub use_default_alerts: bool,
    pub series_master_id: Option<String>,
    pub event_type: EventType,
    pub original_start: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    SingleInstance,
    SeriesMaster,
    Occurrence,
    Exception,
}

pub fn classify_event_type(value: &Value) -> EventType {
    match value.get("type").and_then(Value::as_str).unwrap_or("") {
        "seriesMaster" => EventType::SeriesMaster,
        "occurrence" => EventType::Occurrence,
        "exception" => EventType::Exception,
        _ => EventType::SingleInstance,
    }
}

pub fn convert_event(
    graph_event: &Value,
    fallback_calendar_tz: Option<&str>,
) -> Result<ConvertedEvent, GraphError> {
    let uid = graph_event
        .get("iCalUId")
        .and_then(Value::as_str)
        .or_else(|| graph_event.get("id").and_then(Value::as_str))
        .ok_or_else(|| GraphError::Malformed("event has neither iCalUId nor id".to_owned()))?
        .to_owned();

    let event_type = classify_event_type(graph_event);
    let series_master_id = graph_event
        .get("seriesMasterId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let original_start = graph_event
        .get("originalStart")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let is_draft = graph_event
        .get("isDraft")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut card = Map::new();
    card.insert("@type".to_owned(), Value::from("Event"));
    card.insert("uid".to_owned(), Value::from(uid.clone()));

    if let Some(subject) = graph_event.get("subject").and_then(Value::as_str)
        && !subject.is_empty()
    {
        card.insert("title".to_owned(), Value::from(subject.to_owned()));
    }

    if let Some(body) = graph_event.get("body").and_then(Value::as_object)
        && let Some(content) = body.get("content").and_then(Value::as_str)
        && !content.is_empty()
    {
        card.insert("description".to_owned(), Value::from(content.to_owned()));
    }

    let start_dt = extract_local_datetime(graph_event.get("start")).map(strip_fractional);
    let end_dt = extract_local_datetime(graph_event.get("end")).map(strip_fractional);

    let is_all_day = graph_event
        .get("isAllDay")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let display_tz = graph_event
        .get("originalStartTimeZone")
        .and_then(Value::as_str)
        .and_then(windows_or_iana_to_iana)
        .or_else(|| {
            graph_event
                .get("start")
                .and_then(|s| s.get("timeZone"))
                .and_then(Value::as_str)
                .and_then(windows_or_iana_to_iana)
        })
        .or_else(|| fallback_calendar_tz.and_then(windows_or_iana_to_iana))
        .filter(|z| z != "Etc/UTC");

    if let Some(start) = start_dt.as_deref() {
        let value = match (is_all_day, display_tz.as_deref()) {
            (false, Some(tz)) => utc_naive_to_local(start, tz).unwrap_or_else(|| start.to_owned()),
            _ => start.to_owned(),
        };
        card.insert("start".to_owned(), Value::from(value));
    }

    if let (Some(start), Some(end)) = (start_dt.as_deref(), end_dt.as_deref())
        && let Some(dur) = duration_iso8601(start, end)
    {
        card.insert("duration".to_owned(), Value::from(dur));
    }

    if is_all_day {
        card.insert("showWithoutTime".to_owned(), Value::Bool(true));
    } else {
        let label = display_tz.unwrap_or_else(|| "Etc/UTC".to_owned());
        card.insert("timeZone".to_owned(), Value::from(label));
    }

    if let Some(true) = graph_event.get("isCancelled").and_then(Value::as_bool) {
        card.insert("status".to_owned(), Value::from("cancelled"));
    } else {
        card.insert("status".to_owned(), Value::from("confirmed"));
    }

    if let Some(sens) = graph_event.get("sensitivity").and_then(Value::as_str) {
        card.insert("privacy".to_owned(), Value::from(privacy_for(sens)));
    }

    if let Some(imp) = graph_event.get("importance").and_then(Value::as_str) {
        card.insert("priority".to_owned(), Value::from(priority_for(imp)));
    }

    if let Some(show) = graph_event.get("showAs").and_then(Value::as_str) {
        card.insert(
            "freeBusyStatus".to_owned(),
            Value::from(free_busy_for(show)),
        );
    }

    if let Some(cats) = graph_event.get("categories").and_then(Value::as_array)
        && !cats.is_empty()
    {
        let mut map = Map::new();
        for cat in cats.iter().filter_map(Value::as_str) {
            map.insert(cat.to_owned(), Value::Bool(true));
        }
        if !map.is_empty() {
            card.insert("keywords".to_owned(), Value::Object(map));
        }
    }

    if let Some(locs) = graph_event.get("locations").and_then(Value::as_array)
        && !locs.is_empty()
    {
        let mut locations = Map::new();
        let width = pad_width(locs.len());
        for (i, loc) in locs.iter().enumerate() {
            let key = format!("loc-{:0width$}", i + 1, width = width);
            let mut object = Map::new();
            object.insert("@type".to_owned(), Value::from("Location"));
            let display_name = loc
                .get("displayName")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            let address_str = loc
                .get("address")
                .and_then(Value::as_object)
                .map(|addr| {
                    ["street", "city", "state", "postalCode", "countryOrRegion"]
                        .iter()
                        .filter_map(|f| addr.get(*f).and_then(Value::as_str))
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|s| !s.is_empty());
            let name = match (display_name, address_str.as_deref()) {
                (Some(dn), Some(addr)) if dn != addr => Some(format!("{dn}, {addr}")),
                (Some(dn), _) => Some(dn.to_owned()),
                (None, Some(addr)) => Some(addr.to_owned()),
                (None, None) => None,
            };
            if let Some(n) = name {
                object.insert("name".to_owned(), Value::from(n));
            }
            locations.insert(key, Value::Object(object));
        }
        if !locations.is_empty() {
            card.insert("locations".to_owned(), Value::Object(locations));
        }
    }

    if let Some(recurrence) = graph_event.get("recurrence")
        && !recurrence.is_null()
    {
        let rule = convert_patterned_recurrence_rule(recurrence)?;
        card.insert("recurrenceRule".to_owned(), Value::Object(rule));
    }

    let mut participants: Vec<(String, Value)> = Vec::new();
    if let Some(org) = graph_event
        .get("organizer")
        .and_then(|o| o.get("emailAddress"))
    {
        if let Some(addr) = org.get("address").and_then(Value::as_str)
            && !addr.is_empty()
        {
            card.insert(
                "organizerCalendarAddress".to_owned(),
                Value::from(format!("mailto:{addr}")),
            );
        }
        let p = build_participant(org, &["owner", "chair"], None, None);
        participants.push(("organizer".to_owned(), p));
    }
    if let Some(attendees) = graph_event.get("attendees").and_then(Value::as_array) {
        let width = pad_width(attendees.len());
        for (i, att) in attendees.iter().enumerate() {
            let key = format!("att-{:0width$}", i + 1, width = width);
            let Some(email) = att.get("emailAddress") else {
                continue;
            };
            let role = att
                .get("type")
                .and_then(Value::as_str)
                .map(attendee_type_to_role)
                .unwrap_or("required");
            let status = att
                .get("status")
                .and_then(|s| s.get("response"))
                .and_then(Value::as_str)
                .and_then(attendee_response_to_status);
            let p = build_participant(email, &[role], status, Some(true));
            participants.push((key, p));
        }
    }
    if !participants.is_empty() {
        let map: Map<String, Value> = participants.into_iter().collect();
        card.insert("participants".to_owned(), Value::Object(map));
    }

    if let Some(created) = graph_event.get("createdDateTime").and_then(Value::as_str) {
        card.insert(
            "created".to_owned(),
            Value::from(strip_fractional_utc(created)),
        );
    }
    if let Some(updated) = graph_event
        .get("lastModifiedDateTime")
        .and_then(Value::as_str)
    {
        card.insert(
            "updated".to_owned(),
            Value::from(strip_fractional_utc(updated)),
        );
    }

    let use_default_alerts = false;
    if let Some(true) = graph_event.get("isReminderOn").and_then(Value::as_bool)
        && let Some(mins) = graph_event
            .get("reminderMinutesBeforeStart")
            .and_then(Value::as_i64)
    {
        let trigger = format!("-PT{}M", mins.unsigned_abs());
        let alert = json!({
            "@type": "Alert",
            "trigger": {
                "@type": "OffsetTrigger",
                "relativeTo": "start",
                "offset": trigger
            }
        });
        let mut alerts = Map::new();
        alerts.insert("alert-1".to_owned(), alert);
        card.insert("alerts".to_owned(), Value::Object(alerts));
    }

    if let Some(cancels) = graph_event.get("cancelledOccurrences")
        && !cancels.is_null()
    {
        let cancels = cancels.as_array().ok_or_else(|| {
            GraphError::Malformed("event.cancelledOccurrences is not an array".to_owned())
        })?;
        let mut overrides = Map::new();
        for entry in cancels {
            let occurrence_id = entry.as_str().ok_or_else(|| {
                GraphError::Malformed(
                    "event.cancelledOccurrences contains a non-string value".to_owned(),
                )
            })?;
            let key = cancelled_occurrence_key(
                graph_event,
                occurrence_id,
                is_all_day,
                fallback_calendar_tz,
            )?;
            overrides.insert(key, json!({"excluded": true}));
        }
        if !overrides.is_empty() {
            card.insert("recurrenceOverrides".to_owned(), Value::Object(overrides));
        }
    }

    Ok(ConvertedEvent {
        uid,
        data: Value::Object(card),
        is_draft,
        use_default_alerts,
        series_master_id,
        event_type,
        original_start,
    })
}

/// Convert Graph's opaque occurrence identifier into the LocalDateTime key
/// required by JSCalendar's `recurrenceOverrides` map.
///
/// Graph documents the identifier as
/// `OID.{seriesMasterId}.{occurrence-start-date}`.  The date is expressed in
/// the recurrence range's time zone, while the occurrence's wall-clock time is
/// inherited from the series master.  Copying the opaque identifier verbatim
/// produces a key calcard (and therefore Stalwart) silently ignores, reviving a
/// cancelled instance.  Reject every shape we cannot prove instead.
fn cancelled_occurrence_key(
    graph_event: &Value,
    occurrence_id: &str,
    is_all_day: bool,
    fallback_calendar_tz: Option<&str>,
) -> Result<String, GraphError> {
    if classify_event_type(graph_event) != EventType::SeriesMaster {
        return Err(GraphError::Malformed(
            "cancelledOccurrences is present on a non-series-master event".to_owned(),
        ));
    }

    let master_id = graph_event
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            GraphError::Malformed(
                "series master with cancelledOccurrences has no non-empty id".to_owned(),
            )
        })?;
    let encoded = occurrence_id.strip_prefix("OID.").ok_or_else(|| {
        GraphError::Malformed(format!(
            "cancelled occurrence id {occurrence_id:?} does not start with OID."
        ))
    })?;
    let (encoded_master, raw_date) = encoded.rsplit_once('.').ok_or_else(|| {
        GraphError::Malformed(format!(
            "cancelled occurrence id {occurrence_id:?} has no start-date suffix"
        ))
    })?;
    if encoded_master != master_id {
        return Err(GraphError::Malformed(format!(
            "cancelled occurrence id {occurrence_id:?} belongs to series {encoded_master:?}, not {master_id:?}"
        )));
    }

    let date = chrono::NaiveDate::parse_from_str(raw_date, "%Y-%m-%d").map_err(|_| {
        GraphError::Malformed(format!(
            "cancelled occurrence id {occurrence_id:?} has invalid start date {raw_date:?}"
        ))
    })?;
    let time = if is_all_day {
        chrono::NaiveTime::MIN
    } else {
        master_recurrence_local_time(graph_event, fallback_calendar_tz)?
    };

    Ok(date.and_time(time).format("%Y-%m-%dT%H:%M:%S").to_string())
}

/// Resolve the series master's wall-clock time in the recurrence range zone.
/// The Graph import asks for UTC event timestamps, but this also honours the
/// `start.timeZone` value if Graph returns another zone.
fn master_recurrence_local_time(
    graph_event: &Value,
    fallback_calendar_tz: Option<&str>,
) -> Result<chrono::NaiveTime, GraphError> {
    use chrono::{DateTime, NaiveDateTime, TimeZone};
    use chrono_tz::Tz;

    let recurrence_tz_name = graph_event
        .get("recurrence")
        .and_then(|r| r.get("range"))
        .and_then(|r| r.get("recurrenceTimeZone"))
        .and_then(Value::as_str)
        .and_then(windows_or_iana_to_iana)
        .or_else(|| {
            graph_event
                .get("originalStartTimeZone")
                .and_then(Value::as_str)
                .and_then(windows_or_iana_to_iana)
        })
        .or_else(|| {
            graph_event
                .get("start")
                .and_then(|s| s.get("timeZone"))
                .and_then(Value::as_str)
                .and_then(windows_or_iana_to_iana)
        })
        .or_else(|| fallback_calendar_tz.and_then(windows_or_iana_to_iana))
        .ok_or_else(|| {
            GraphError::Malformed(
                "series master with cancelledOccurrences has no supported recurrence time zone"
                    .to_owned(),
            )
        })?;
    let recurrence_tz: Tz = recurrence_tz_name.parse().map_err(|_| {
        GraphError::Malformed(format!(
            "series master recurrence time zone {recurrence_tz_name:?} is not IANA"
        ))
    })?;

    let start = graph_event
        .get("start")
        .and_then(|s| s.get("dateTime"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            GraphError::Malformed(
                "series master with cancelledOccurrences has no start.dateTime".to_owned(),
            )
        })?;

    let local = if let Ok(with_offset) = DateTime::parse_from_rfc3339(start) {
        with_offset.with_timezone(&recurrence_tz).naive_local()
    } else {
        let naive = NaiveDateTime::parse_from_str(start, "%Y-%m-%dT%H:%M:%S%.f").map_err(|_| {
            GraphError::Malformed(format!(
                "series master start.dateTime {start:?} is not an ISO local date-time"
            ))
        })?;
        let source_tz_name = graph_event
            .get("start")
            .and_then(|s| s.get("timeZone"))
            .and_then(Value::as_str)
            .and_then(windows_or_iana_to_iana)
            .unwrap_or_else(|| recurrence_tz_name.clone());
        let source_tz: Tz = source_tz_name.parse().map_err(|_| {
            GraphError::Malformed(format!(
                "series master start time zone {source_tz_name:?} is not IANA"
            ))
        })?;
        source_tz
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| {
                GraphError::Malformed(format!(
                    "series master start.dateTime {start:?} is ambiguous or nonexistent in {source_tz_name}"
                ))
            })?
            .with_timezone(&recurrence_tz)
            .naive_local()
    };

    Ok(local.time())
}

fn extract_local_datetime(slot: Option<&Value>) -> Option<String> {
    let slot = slot?;
    let dt = slot.get("dateTime").and_then(Value::as_str)?;
    Some(dt.to_owned())
}

fn build_participant(
    email: &Value,
    roles: &[&str],
    participation_status: Option<&'static str>,
    expect_reply: Option<bool>,
) -> Value {
    let mut map = Map::new();
    map.insert("@type".to_owned(), Value::from("Participant"));
    if let Some(addr) = email.get("address").and_then(Value::as_str) {
        map.insert(
            "calendarAddress".to_owned(),
            Value::from(format!("mailto:{addr}")),
        );
        map.insert("email".to_owned(), Value::from(addr.to_owned()));
    }
    if let Some(name) = email.get("name").and_then(Value::as_str)
        && !name.is_empty()
    {
        map.insert("name".to_owned(), Value::from(name.to_owned()));
    }
    let role_map: Map<String, Value> = roles
        .iter()
        .map(|r| ((*r).to_owned(), Value::Bool(true)))
        .collect();
    map.insert("roles".to_owned(), Value::Object(role_map));
    if let Some(status) = participation_status {
        map.insert(
            "participationStatus".to_owned(),
            Value::from(status.to_owned()),
        );
    }
    if let Some(b) = expect_reply {
        map.insert("expectReply".to_owned(), Value::Bool(b));
    }
    Value::Object(map)
}

fn attendee_type_to_role(t: &str) -> &'static str {
    match t {
        "optional" => "optional",
        "resource" => "informational",
        _ => "required",
    }
}

fn attendee_response_to_status(r: &str) -> Option<&'static str> {
    match r {
        "accepted" | "organizer" => Some("accepted"),
        "declined" => Some("declined"),
        "tentativelyAccepted" => Some("tentative"),
        "notResponded" | "none" => Some("needs-action"),
        _ => None,
    }
}

fn pad_width(n: usize) -> usize {
    let mut digits = 1;
    let mut k = n;
    while k >= 10 {
        digits += 1;
        k /= 10;
    }
    digits
}

fn privacy_for(value: &str) -> &'static str {
    match value {
        "private" => "private",
        "confidential" => "secret",
        _ => "public",
    }
}

fn priority_for(value: &str) -> i64 {
    match value {
        "high" => 1,
        "low" => 9,
        _ => 5,
    }
}

fn free_busy_for(value: &str) -> &'static str {
    match value {
        "free" => "free",
        _ => "busy",
    }
}

fn duration_iso8601(start: &str, end: &str) -> Option<String> {
    let s = parse_naive_seconds(start)?;
    let e = parse_naive_seconds(end)?;
    if e <= s {
        return None;
    }
    let total = e - s;
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    let mut out = "PT".to_owned();
    use std::fmt::Write;
    if hours > 0 {
        let _ = write!(out, "{hours}H");
    }
    if minutes > 0 || (hours > 0 && seconds > 0) {
        let _ = write!(out, "{minutes}M");
    }
    if seconds > 0 || (hours == 0 && minutes == 0) {
        let _ = write!(out, "{seconds}S");
    }
    Some(out)
}

fn strip_fractional(s: String) -> String {
    if let Some(dot) = s.find('.') {
        s[..dot].to_owned()
    } else {
        s
    }
}

fn strip_fractional_utc(raw: &str) -> String {
    let trailing_z = raw.ends_with('Z');
    let trimmed = raw.trim_end_matches('Z');
    let base = match trimmed.find('.') {
        Some(dot) => &trimmed[..dot],
        None => trimmed,
    };
    if trailing_z {
        format!("{base}Z")
    } else {
        base.to_owned()
    }
}

fn parse_naive_seconds(s: &str) -> Option<i64> {
    let (date, time) = s.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    let time = time.split('.').next()?;
    let time = time.trim_end_matches('Z');
    let mut time_parts = time.split(':');
    let hh: i64 = time_parts.next()?.parse().ok()?;
    let mm: i64 = time_parts.next()?.parse().ok()?;
    let ss: i64 = time_parts.next().unwrap_or("0").parse().ok()?;
    Some(days_from_civil(year, month, day) * 86400 + hh * 3600 + mm * 60 + ss)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let m_adj: i64 = if m > 2 { m - 3 } else { m + 9 };
    let doy = ((153 * m_adj + 2) / 5 + d - 1) as u64;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + (doe as i64) - 719468
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!({
            "id": "AAA",
            "iCalUId": "uid-1",
            "type": "singleInstance",
            "subject": "Sync up",
            "body": {"contentType": "text", "content": "Agenda"},
            "start": {"dateTime": "2026-05-27T10:00:00.0000000", "timeZone": "UTC"},
            "end": {"dateTime": "2026-05-27T11:00:00.0000000", "timeZone": "UTC"},
            "isAllDay": false,
            "isCancelled": false,
            "sensitivity": "private",
            "importance": "high",
            "showAs": "busy",
            "categories": ["Red", "Internal"],
            "createdDateTime": "2026-05-01T08:00:00Z",
            "lastModifiedDateTime": "2026-05-02T08:00:00Z",
            "isReminderOn": true,
            "reminderMinutesBeforeStart": 15,
            "isDraft": false,
            "organizer": {"emailAddress": {"name": "Alice", "address": "alice@x.com"}},
            "attendees": [
                {"emailAddress": {"name": "Bob", "address": "bob@x.com"}, "type": "required"}
            ]
        })
    }

    #[test]
    fn classify_event_type_dispatch() {
        assert_eq!(
            classify_event_type(&json!({"type": "seriesMaster"})),
            EventType::SeriesMaster
        );
        assert_eq!(
            classify_event_type(&json!({"type": "occurrence"})),
            EventType::Occurrence
        );
        assert_eq!(
            classify_event_type(&json!({"type": "exception"})),
            EventType::Exception
        );
        assert_eq!(classify_event_type(&json!({})), EventType::SingleInstance);
    }

    #[test]
    fn single_instance_converts_to_event_value() {
        let conv = convert_event(&sample(), Some("UTC")).unwrap();
        assert_eq!(conv.uid, "uid-1");
        assert_eq!(conv.data["@type"], "Event");
        assert_eq!(conv.data["title"], "Sync up");
        assert_eq!(conv.data["description"], "Agenda");
        assert_eq!(conv.data["start"], "2026-05-27T10:00:00");
        assert_eq!(conv.data["duration"], "PT1H");
        assert_eq!(conv.data["status"], "confirmed");
        assert_eq!(conv.data["privacy"], "private");
        assert_eq!(conv.data["priority"], 1);
        assert_eq!(conv.data["freeBusyStatus"], "busy");
        let kws = conv.data["keywords"].as_object().unwrap();
        assert!(kws.contains_key("Red"));
        assert!(kws.contains_key("Internal"));
        assert!(conv.data.get("categories").is_none());
        assert!(conv.data.get("version").is_none());
        assert_eq!(conv.data["timeZone"], "Etc/UTC");
        let alerts = conv.data["alerts"].as_object().unwrap();
        assert_eq!(alerts["alert-1"]["trigger"]["offset"], "-PT15M");
    }

    #[test]
    fn cancelled_event_status_is_cancelled() {
        let mut v = sample();
        v["isCancelled"] = Value::Bool(true);
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["status"], "cancelled");
    }

    #[test]
    fn all_day_sets_show_without_time() {
        let mut v = sample();
        v["isAllDay"] = Value::Bool(true);
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["showWithoutTime"], true);
    }

    #[test]
    fn priority_mapping_covers_three_levels() {
        let conv = convert_event(&sample(), None).unwrap();
        assert_eq!(conv.data["priority"], 1);
        let mut v = sample();
        v["importance"] = Value::from("normal");
        assert_eq!(convert_event(&v, None).unwrap().data["priority"], 5);
        v["importance"] = Value::from("low");
        assert_eq!(convert_event(&v, None).unwrap().data["priority"], 9);
    }

    #[test]
    fn duration_handles_subhour() {
        assert_eq!(
            duration_iso8601("2026-05-27T10:00:00", "2026-05-27T10:30:00").as_deref(),
            Some("PT30M")
        );
    }

    #[test]
    fn duration_handles_multi_day() {
        assert_eq!(
            duration_iso8601("2026-05-27T10:00:00", "2026-05-28T11:30:45").as_deref(),
            Some("PT25H30M45S")
        );
    }

    #[test]
    fn windows_tz_fallback_returns_iana() {
        assert_eq!(
            windows_or_iana_to_iana("Pacific Standard Time").as_deref(),
            Some("America/Los_Angeles")
        );
        assert_eq!(
            windows_or_iana_to_iana("America/New_York").as_deref(),
            Some("America/New_York")
        );
        assert!(windows_or_iana_to_iana("Made Up Zone").is_none());
    }

    #[test]
    fn organizer_and_attendees_become_participants() {
        let conv = convert_event(&sample(), None).unwrap();
        let participants = conv.data["participants"].as_object().unwrap();
        assert!(participants.contains_key("organizer"));
        assert_eq!(participants["organizer"]["email"], "alice@x.com");
        assert_eq!(
            participants["organizer"]["calendarAddress"],
            "mailto:alice@x.com"
        );
        assert_eq!(participants["organizer"]["roles"]["owner"], true);
        assert_eq!(participants["organizer"]["roles"]["chair"], true);
        assert!(participants.contains_key("att-1"));
        assert_eq!(participants["att-1"]["email"], "bob@x.com");
        assert_eq!(participants["att-1"]["roles"]["required"], true);
    }

    #[test]
    fn optional_attendee_keeps_role_and_uses_status_when_present() {
        let mut v = sample();
        v["attendees"] = json!([
            {
                "emailAddress": {"name": "Carol", "address": "carol@x.com"},
                "type": "optional",
                "status": {"response": "tentativelyAccepted"}
            }
        ]);
        let conv = convert_event(&v, None).unwrap();
        let p = &conv.data["participants"]["att-1"];
        assert_eq!(p["roles"]["optional"], true);
        assert!(p["roles"].get("required").is_none());
        assert_eq!(p["participationStatus"], "tentative");
    }

    #[test]
    fn many_attendees_zero_pad_keys_for_stable_sort() {
        let mut v = sample();
        let mut atts = Vec::new();
        for i in 1..=11 {
            atts.push(json!({
                "emailAddress": {"address": format!("u{i}@x.com")},
                "type": "required"
            }));
        }
        v["attendees"] = Value::Array(atts);
        let conv = convert_event(&v, None).unwrap();
        let map = conv.data["participants"].as_object().unwrap();
        assert!(map.contains_key("att-01"));
        assert!(map.contains_key("att-11"));
    }

    #[test]
    fn original_start_time_zone_is_preserved() {
        let mut v = sample();
        v["originalStartTimeZone"] = Value::from("Pacific Standard Time");
        v["start"]["timeZone"] = Value::from("UTC");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["timeZone"], "America/Los_Angeles");
        assert_eq!(conv.data["start"], "2026-05-27T03:00:00");
    }

    #[test]
    fn utc_times_are_converted_into_the_original_zone() {
        let mut v = sample();
        v["originalStartTimeZone"] = Value::from("Europe/Paris");
        v["start"]["dateTime"] = Value::from("2025-03-12T07:30:00.0000000");
        v["start"]["timeZone"] = Value::from("UTC");
        v["end"]["dateTime"] = Value::from("2025-03-12T08:00:00.0000000");
        v["end"]["timeZone"] = Value::from("UTC");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["start"], "2025-03-12T08:30:00");
        assert_eq!(conv.data["timeZone"], "Europe/Paris");
        assert_eq!(conv.data["duration"], "PT30M");
    }

    #[test]
    fn summer_event_shifts_by_two_hours_in_paris() {
        let mut v = sample();
        v["originalStartTimeZone"] = Value::from("Romance Standard Time");
        v["start"]["dateTime"] = Value::from("2025-07-15T06:30:00.0000000");
        v["start"]["timeZone"] = Value::from("UTC");
        v["end"]["dateTime"] = Value::from("2025-07-15T07:30:00.0000000");
        v["end"]["timeZone"] = Value::from("UTC");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["start"], "2025-07-15T08:30:00");
        assert_eq!(conv.data["timeZone"], "Europe/Paris");
    }

    #[test]
    fn unresolvable_microsoft_zone_falls_back_to_utc() {
        let mut v = sample();
        v["originalStartTimeZone"] = Value::from("tzone://Microsoft/Custom");
        v["start"]["dateTime"] = Value::from("2025-03-12T07:30:00.0000000");
        v["start"]["timeZone"] = Value::from("UTC");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["start"], "2025-03-12T07:30:00");
        assert_eq!(conv.data["timeZone"], "Etc/UTC");
    }

    #[test]
    fn all_day_keeps_date_and_omits_timezone() {
        let mut v = sample();
        v["isAllDay"] = Value::Bool(true);
        v["originalStartTimeZone"] = Value::from("Europe/Paris");
        v["start"]["dateTime"] = Value::from("2025-03-12T00:00:00.0000000");
        v["start"]["timeZone"] = Value::from("UTC");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["start"], "2025-03-12T00:00:00");
        assert_eq!(conv.data["showWithoutTime"], true);
        assert!(conv.data.get("timeZone").is_none());
    }

    #[test]
    fn duration_h_and_s_no_minutes_includes_zero_m() {
        assert_eq!(
            duration_iso8601("2026-05-27T10:00:00", "2026-05-27T11:00:05").as_deref(),
            Some("PT1H0M5S")
        );
    }

    #[test]
    fn fractional_seconds_are_stripped_from_timestamps() {
        let mut v = sample();
        v["createdDateTime"] = Value::from("2026-05-01T08:00:00.0000000Z");
        v["lastModifiedDateTime"] = Value::from("2026-05-02T08:00:00.123Z");
        let conv = convert_event(&v, None).unwrap();
        assert_eq!(conv.data["created"], "2026-05-01T08:00:00Z");
        assert_eq!(conv.data["updated"], "2026-05-02T08:00:00Z");
    }

    #[test]
    fn organizer_calendar_address_is_set_alongside_participants() {
        let conv = convert_event(&sample(), None).unwrap();
        assert_eq!(conv.data["organizerCalendarAddress"], "mailto:alice@x.com");
    }

    #[test]
    fn recurrence_emits_singular_recurrence_rule_object() {
        let mut v = sample();
        v["recurrence"] = json!({
            "pattern": {"type": "daily", "interval": 1},
            "range": {"type": "noEnd"}
        });
        let conv = convert_event(&v, None).unwrap();
        assert!(conv.data.get("recurrenceRules").is_none());
        let rule = &conv.data["recurrenceRule"];
        assert!(rule.is_object());
        assert_eq!(rule["@type"], "RecurrenceRule");
        assert_eq!(rule["frequency"], "daily");
    }

    #[test]
    fn cancelled_occurrence_id_becomes_a_local_datetime_override_key() {
        let mut v = sample();
        v["type"] = Value::from("seriesMaster");
        v["recurrence"] = json!({
            "pattern": {"type": "daily", "interval": 1},
            "range": {
                "type": "noEnd",
                "startDate": "2025-01-15",
                "recurrenceTimeZone": "Romance Standard Time"
            }
        });
        v["originalStartTimeZone"] = Value::from("Romance Standard Time");
        v["start"] = json!({"dateTime": "2025-01-15T08:30:00.0000000", "timeZone": "UTC"});
        v["end"] = json!({"dateTime": "2025-01-15T09:00:00.0000000", "timeZone": "UTC"});
        v["cancelledOccurrences"] = json!(["OID.AAA.2025-07-15"]);

        let conv = convert_event(&v, None).expect("valid cancelled occurrence");
        let overrides = conv.data["recurrenceOverrides"].as_object().unwrap();
        assert_eq!(
            overrides.get("2025-07-15T09:30:00"),
            Some(&json!({"excluded": true})),
            "the cancellation keeps the master's 09:30 recurrence wall time across DST"
        );
        assert!(
            !overrides.contains_key("OID.AAA.2025-07-15"),
            "Graph's opaque occurrenceId is not a valid JSCalendar key"
        );
    }

    #[test]
    fn cancelled_occurrence_survives_the_same_calcard_roundtrip_as_stalwart() {
        use calcard::jscalendar::JSCalendar;

        let mut v = sample();
        v["type"] = Value::from("seriesMaster");
        v["recurrence"] = json!({
            "pattern": {"type": "daily", "interval": 1},
            "range": {
                "type": "numbered",
                "startDate": "2026-05-27",
                "numberOfOccurrences": 4,
                "recurrenceTimeZone": "UTC"
            }
        });
        v["cancelledOccurrences"] = json!(["OID.AAA.2026-05-29"]);

        let conv = convert_event(&v, None).expect("convert Graph series");
        // Stalwart applies CalendarEvent/set properties to a JSCalendar Group,
        // then converts that group through calcard before storing iCalendar.
        let encoded = json!({"@type": "Group", "entries": [conv.data]}).to_string();
        let js = JSCalendar::<String, String>::parse(&encoded).expect("parse JSCalendar");
        let ical = js.into_icalendar().expect("Stalwart calcard conversion");
        let wire = ical.to_string();
        assert!(wire.contains("EXDATE"), "{wire}");
        assert!(wire.contains("20260529T100000"), "{wire}");

        let roundtrip = ical.into_jscalendar::<String, String>();
        let roundtrip: Value =
            serde_json::from_str(&roundtrip.to_string_pretty()).expect("round-trip JSON");
        assert_eq!(
            roundtrip["entries"][0]["recurrenceOverrides"]["2026-05-29T10:00:00"]["excluded"], true,
            "{roundtrip:#}"
        );
    }

    #[test]
    fn malformed_cancelled_occurrence_fails_closed() {
        let mut v = sample();
        v["type"] = Value::from("seriesMaster");
        v["recurrence"] = json!({
            "pattern": {"type": "daily", "interval": 1},
            "range": {
                "type": "noEnd",
                "startDate": "2026-05-27",
                "recurrenceTimeZone": "UTC"
            }
        });
        v["cancelledOccurrences"] = json!(["OID.OTHER.not-a-date"]);

        let err = convert_event(&v, None).expect_err("bad cancellation must reject master");
        assert!(matches!(err, GraphError::Malformed(_)));
    }

    #[test]
    fn location_with_address_only_uses_name_property() {
        let mut v = sample();
        v["locations"] = json!([{
            "address": {"street": "1 Infinite Loop", "city": "Cupertino"}
        }]);
        let conv = convert_event(&v, None).unwrap();
        let locs = conv.data["locations"].as_object().unwrap();
        let only = locs.values().next().unwrap();
        assert_eq!(only["@type"], "Location");
        assert_eq!(only["name"], "1 Infinite Loop, Cupertino");
        assert!(only.get("description").is_none());
    }
}
