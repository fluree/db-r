//! SSE Event Notification Endpoint
//!
//! Publishes nameservice record changes to external query peers.
//! Contract is idempotent (full record) and monotonic (clients react based on `commit_t`/`index_t`).
//!
//! ## Endpoint
//! ```text
//! GET /fluree/events?ledger=books:main&ledger=people:main&vg=my-vg:main
//! GET /fluree/events?all=true
//! ```
//!
//! ## Query Parameter Precedence
//! - `all=true` overrides any `ledger=` or `vg=` params
//! - Otherwise, filter to explicitly provided aliases
//!
//! ## Event Types
//! - `ns-record` - Record published/updated (ledger or VG)
//! - `ns-retracted` - Record retracted/deleted

use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use chrono::Utc;
use fluree_db_nameservice::{
    NameService, NameServiceEvent, NsRecord, Publication, SubscriptionScope, VgNsRecord,
    VirtualGraphPublisher,
};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::broadcast;

use crate::config::{EventsAuthConfig, EventsAuthMode, ServerRole};
use crate::error::ServerError;
use crate::extract::{EventsPrincipal, MaybeBearer};
use crate::state::AppState;

/// Query parameters for the events endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct EventsQuery {
    /// Subscribe to all ledgers and VGs
    #[serde(default)]
    pub all: bool,

    /// Specific ledger aliases to subscribe to
    #[serde(default, rename = "ledger")]
    pub ledgers: Vec<String>,

    /// Specific virtual graph aliases to subscribe to
    #[serde(default, rename = "vg")]
    pub vgs: Vec<String>,
}

impl EventsQuery {
    /// Check if this query matches a given alias and kind
    #[cfg(test)]
    pub fn matches(&self, alias: &str, kind: &str) -> bool {
        if self.all {
            return true;
        }
        match kind {
            "ledger" => self.ledgers.iter().any(|l| l == alias),
            "virtual-graph" => self.vgs.iter().any(|v| v == alias),
            _ => false,
        }
    }
}

/// SSE event data payload for ns-record events
#[derive(Debug, Serialize)]
struct NsRecordData {
    action: &'static str,
    kind: &'static str,
    alias: String,
    record: serde_json::Value,
    emitted_at: String,
}

/// SSE event data payload for ns-retracted events
#[derive(Debug, Serialize)]
struct NsRetractedData {
    action: &'static str,
    kind: &'static str,
    alias: String,
    emitted_at: String,
}

/// Compute the SSE event ID for a ledger record
fn ledger_event_id(alias: &str, record: &NsRecord) -> String {
    format!("ledger:{}:{}:{}", alias, record.commit_t, record.index_t)
}

/// Compute the SSE event ID for a VG record
fn vg_event_id(alias: &str, record: &VgNsRecord) -> String {
    // Use index_t + 8-char truncated SHA-256 of config
    let config_hash = sha256_short(&record.config);
    format!(
        "virtual-graph:{}:{}:{}",
        alias, record.index_t, &config_hash
    )
}

/// Compute the SSE event ID for a retraction
fn retracted_event_id(kind: &str, alias: &str) -> String {
    // Include timestamp for ordering across delete/recreate cycles
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}:{}:retracted:{}", kind, alias, timestamp_ms)
}

/// Compute 8-char truncated SHA-256 hash
fn sha256_short(data: &str) -> String {
    let hash = Sha256::digest(data.as_bytes());
    hex::encode(&hash[..4]) // 4 bytes = 8 hex chars
}

/// Get current ISO-8601 timestamp
fn now_iso8601() -> String {
    Utc::now().to_rfc3339()
}

/// Convert a ledger NsRecord to an SSE Event
fn ledger_to_sse_event(record: &NsRecord) -> Event {
    let alias = format!("{}:{}", record.alias, record.branch);
    let event_id = ledger_event_id(&alias, record);

    let data = NsRecordData {
        action: "ns-record",
        kind: "ledger",
        alias: alias.clone(),
        record: serde_json::json!({
            "alias": alias,
            "branch": record.branch,
            "commit_address": record.commit_address,
            "commit_t": record.commit_t,
            "index_address": record.index_address,
            "index_t": record.index_t,
            "retracted": record.retracted,
        }),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-record")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Convert a VG record to an SSE Event
fn vg_to_sse_event(record: &VgNsRecord) -> Event {
    let alias = record.alias();
    let event_id = vg_event_id(&alias, record);

    let data = NsRecordData {
        action: "ns-record",
        kind: "virtual-graph",
        alias: alias.clone(),
        record: serde_json::json!({
            "alias": alias,
            "name": record.name,
            "branch": record.branch,
            "vg_type": record.vg_type.to_type_string(),
            "config": record.config,
            "dependencies": record.dependencies,
            "index_address": record.index_address,
            "index_t": record.index_t,
            "retracted": record.retracted,
        }),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-record")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Create a retracted SSE Event
fn retracted_sse_event(kind: &'static str, alias: &str) -> Event {
    let event_id = retracted_event_id(kind, alias);

    let data = NsRetractedData {
        action: "ns-retracted",
        kind,
        alias: alias.to_string(),
        emitted_at: now_iso8601(),
    };

    Event::default()
        .event("ns-retracted")
        .id(event_id)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

/// Build the initial snapshot of records on connection
async fn build_initial_snapshot<N>(ns: &N, params: &EventsQuery) -> Vec<Event>
where
    N: NameService + VirtualGraphPublisher,
{
    let mut events = Vec::new();

    if params.all {
        // All ledger records (sorted by alias)
        if let Ok(mut records) = ns.all_records().await {
            records.sort_by(|a, b| {
                let a_alias = format!("{}:{}", a.alias, a.branch);
                let b_alias = format!("{}:{}", b.alias, b.branch);
                a_alias.cmp(&b_alias)
            });
            for r in records {
                if !r.retracted {
                    events.push(ledger_to_sse_event(&r));
                }
            }
        }
        // All VG records (sorted by alias)
        if let Ok(mut records) = ns.all_vg_records().await {
            records.sort_by(|a, b| a.alias().cmp(&b.alias()));
            for r in records {
                if !r.retracted {
                    events.push(vg_to_sse_event(&r));
                }
            }
        }
    } else {
        // Requested ledgers (skip missing, sorted, deduped)
        let mut ledger_aliases: Vec<_> = params.ledgers.iter().collect();
        ledger_aliases.sort();
        ledger_aliases.dedup();
        for alias in ledger_aliases {
            if let Ok(Some(r)) = ns.lookup(alias).await {
                if !r.retracted {
                    events.push(ledger_to_sse_event(&r));
                }
            }
        }

        // Requested VGs (skip missing, sorted, deduped)
        let mut vg_aliases: Vec<_> = params.vgs.iter().collect();
        vg_aliases.sort();
        vg_aliases.dedup();
        for alias in vg_aliases {
            if let Ok(Some(r)) = ns.lookup_vg(alias).await {
                if !r.retracted {
                    events.push(vg_to_sse_event(&r));
                }
            }
        }
    }

    events
}

/// Extract the alias from a NameServiceEvent
fn event_alias(event: &NameServiceEvent) -> &str {
    match event {
        NameServiceEvent::LedgerCommitPublished { alias, .. } => alias,
        NameServiceEvent::LedgerIndexPublished { alias, .. } => alias,
        NameServiceEvent::LedgerRetracted { alias } => alias,
        NameServiceEvent::VgConfigPublished { alias, .. } => alias,
        NameServiceEvent::VgIndexPublished { alias, .. } => alias,
        NameServiceEvent::VgSnapshotPublished { alias, .. } => alias,
        NameServiceEvent::VgRetracted { alias } => alias,
    }
}

/// Get the kind (ledger/virtual-graph) from a NameServiceEvent
fn event_kind(event: &NameServiceEvent) -> &'static str {
    match event {
        NameServiceEvent::LedgerCommitPublished { .. }
        | NameServiceEvent::LedgerIndexPublished { .. }
        | NameServiceEvent::LedgerRetracted { .. } => "ledger",
        NameServiceEvent::VgConfigPublished { .. }
        | NameServiceEvent::VgIndexPublished { .. }
        | NameServiceEvent::VgSnapshotPublished { .. }
        | NameServiceEvent::VgRetracted { .. } => "virtual-graph",
    }
}

/// Transform a nameservice event to an SSE Event, fetching the current record
async fn transform_event<N>(ns: &N, event: NameServiceEvent) -> Option<Event>
where
    N: NameService + VirtualGraphPublisher,
{
    let alias = event_alias(&event).to_string();

    match event {
        NameServiceEvent::LedgerCommitPublished { .. }
        | NameServiceEvent::LedgerIndexPublished { .. } => {
            let record = ns.lookup(&alias).await.ok()??;
            Some(ledger_to_sse_event(&record))
        }
        NameServiceEvent::LedgerRetracted { alias } => Some(retracted_sse_event("ledger", &alias)),
        NameServiceEvent::VgConfigPublished { .. }
        | NameServiceEvent::VgIndexPublished { .. }
        | NameServiceEvent::VgSnapshotPublished { .. } => {
            let record = ns.lookup_vg(&alias).await.ok()??;
            Some(vg_to_sse_event(&record))
        }
        NameServiceEvent::VgRetracted { alias } => {
            Some(retracted_sse_event("virtual-graph", &alias))
        }
    }
}

/// Authorize the request and compute effective scope.
///
/// In None mode, params are passed through unchanged.
/// In Optional/Required mode with a token, params are filtered to allowed scope.
fn authorize_request(
    config: &EventsAuthConfig,
    principal: Option<&EventsPrincipal>,
    params: &EventsQuery,
) -> Result<EventsQuery, ServerError> {
    match config.mode {
        EventsAuthMode::None => {
            // No auth: pass through unchanged
            Ok(params.clone())
        }
        EventsAuthMode::Required if principal.is_none() => {
            // Should not reach here (extractor handles it)
            Err(ServerError::unauthorized("Bearer token required"))
        }
        EventsAuthMode::Optional | EventsAuthMode::Required => {
            match principal {
                Some(p) => Ok(filter_to_allowed(params, p)),
                None => Ok(params.clone()), // Optional mode, no token
            }
        }
    }
}

/// Filter requested scope to what principal is allowed.
/// Returns sorted, deduped lists for deterministic behavior.
/// Silently removes disallowed items (no 403, no existence leak).
fn filter_to_allowed(params: &EventsQuery, principal: &EventsPrincipal) -> EventsQuery {
    // If allowed_all is true, ledgers/vgs lists are irrelevant
    if principal.allowed_all {
        // Token grants full access, pass through
        return params.clone();
    }

    // If request.all but token doesn't allow all, expand to allowed lists
    // This is equivalent to all=false with the token's allowed lists
    let mut ledgers: Vec<String> = if params.all {
        principal.allowed_ledgers.iter().cloned().collect()
    } else {
        params
            .ledgers
            .iter()
            .filter(|l| principal.allowed_ledgers.contains(*l))
            .cloned()
            .collect()
    };

    let mut vgs: Vec<String> = if params.all {
        principal.allowed_vgs.iter().cloned().collect()
    } else {
        params
            .vgs
            .iter()
            .filter(|v| principal.allowed_vgs.contains(*v))
            .cloned()
            .collect()
    };

    // Sort and dedup for deterministic snapshots
    ledgers.sort();
    ledgers.dedup();
    vgs.sort();
    vgs.dedup();

    EventsQuery {
        all: false, // Never pass all=true if token restricts
        ledgers,
        vgs,
    }
}

/// SSE events endpoint handler
///
/// Streams nameservice record changes to connected clients.
/// On connect, sends an initial snapshot of all matching records,
/// then streams live updates.
///
/// # Authentication
/// When events authentication is enabled:
/// - `Required` mode: Bearer token must be present and valid
/// - `Optional` mode: Token accepted but not required
/// - `None` mode: Token ignored (default)
pub async fn events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EventsQuery>,
    MaybeBearer(principal): MaybeBearer,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServerError> {
    // Peer mode: return 404 - events endpoint not available
    // Peers subscribe to the transaction server's events endpoint instead
    if state.config.server_role == ServerRole::Peer {
        return Err(ServerError::Api(fluree_db_api::ApiError::NotFound(
            "Events endpoint not available in peer mode".to_string(),
        )));
    }

    // Authorize and compute effective scope
    let effective_params =
        authorize_request(&state.config.events_auth(), principal.as_ref(), &params)?;

    // Log connection (issuer only, never token)
    if let Some(p) = &principal {
        tracing::info!(
            issuer = %p.issuer,
            subject = ?p.subject,
            identity = ?p.identity,
            "SSE connection authorized"
        );
    }

    // Clone nameservice for use in async closures
    // Events endpoint is only available in transaction mode (checked above)
    let ns = state.fluree.as_file().nameservice().clone();

    // 1. SUBSCRIBE FIRST (events during snapshot queue in receiver)
    // This ensures no gap between snapshot and live events
    let subscription = ns
        .subscribe(SubscriptionScope::All)
        .await
        .map_err(|e| ServerError::internal(format!("Failed to subscribe to events: {}", e)))?;

    // 2. Build initial snapshot using effective params
    let initial_events = build_initial_snapshot(&ns, &effective_params).await;
    let initial_stream = stream::iter(initial_events.into_iter().map(Ok));

    // 3. Create live event stream from broadcast receiver using unfold
    let ns_for_live = ns.clone();
    let live_stream = stream::unfold(
        (subscription.receiver, ns_for_live, effective_params),
        |(mut rx, ns_inner, params)| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        // Filter by effective params
                        let alias = event_alias(&event);
                        let kind = event_kind(&event);

                        let matches = if params.all {
                            true
                        } else {
                            match kind {
                                "ledger" => params.ledgers.iter().any(|l| l == alias),
                                "virtual-graph" => params.vgs.iter().any(|v| v == alias),
                                _ => false,
                            }
                        };

                        if !matches {
                            // Skip non-matching events, continue loop
                            continue;
                        }

                        if let Some(sse_event) = transform_event(&ns_inner, event).await {
                            return Some((Ok(sse_event), (rx, ns_inner, params)));
                        }
                        // Event transformed to None (e.g., record not found), continue
                        continue;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            lagged = n,
                            "SSE broadcast lagged, some events may have been missed"
                        );
                        // Continue listening after lag
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end the stream
                        return None;
                    }
                }
            }
        },
    );

    // 4. Chain: snapshot first, then live events
    let combined_stream = initial_stream.chain(live_stream);

    Ok(
        Sse::new(combined_stream)
            .keep_alive(KeepAlive::default().interval(Duration::from_secs(30))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_short() {
        let hash = sha256_short("test config");
        assert_eq!(hash.len(), 8);
    }

    #[test]
    fn test_ledger_event_id() {
        let record = NsRecord {
            address: "test:main".to_string(),
            alias: "test".to_string(),
            branch: "main".to_string(),
            commit_address: Some("commit-addr".to_string()),
            commit_t: 42,
            index_address: Some("index-addr".to_string()),
            index_t: 40,
            default_context_address: None,
            retracted: false,
        };
        let id = ledger_event_id("test:main", &record);
        assert_eq!(id, "ledger:test:main:42:40");
    }

    #[test]
    fn test_retracted_event_id_format() {
        let id = retracted_event_id("ledger", "test:main");
        assert!(id.starts_with("ledger:test:main:retracted:"));
    }

    #[test]
    fn test_events_query_matches() {
        let query = EventsQuery {
            all: false,
            ledgers: vec!["books:main".to_string(), "users:main".to_string()],
            vgs: vec!["search:main".to_string()],
        };

        assert!(query.matches("books:main", "ledger"));
        assert!(query.matches("users:main", "ledger"));
        assert!(!query.matches("other:main", "ledger"));
        assert!(query.matches("search:main", "virtual-graph"));
        assert!(!query.matches("books:main", "virtual-graph"));
    }

    #[test]
    fn test_events_query_matches_all() {
        let query = EventsQuery {
            all: true,
            ledgers: vec![],
            vgs: vec![],
        };

        assert!(query.matches("any:main", "ledger"));
        assert!(query.matches("any:main", "virtual-graph"));
    }
}
