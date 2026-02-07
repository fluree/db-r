//! Query/transaction execution tracking (Clojure parity)
//!
//! This module implements the db-clojure tracking behavior:
//! - Enablement via opts.meta (bool or object) and/or opts.max-fuel
//! - Time returned as formatted string like "12.34ms"
//! - Fuel counted per emitted item; limit errors when total == limit + 1
//! - Policy stats: {policy-id -> {executed, allowed}}

use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Tracking options parsed from query `opts`
#[derive(Debug, Clone, Default)]
pub struct TrackingOptions {
    pub track_time: bool,
    pub track_fuel: bool,
    pub track_policy: bool,
    /// None = unlimited, Some(0) also unlimited
    pub max_fuel: Option<u64>,
}

impl TrackingOptions {
    /// Parse tracking options from a JSON `opts` object value.
    ///
    /// Expected shapes (Clojure parity):
    /// - `"opts": {"meta": true}` enables all tracking
    /// - `"opts": {"meta": {"time": true, "fuel": true, "policy": true}}` selective
    /// - `"opts": {"max-fuel": 1000}` implicitly enables fuel tracking
    ///
    /// Also accepts camel/snake variants for max-fuel (`max_fuel`, `maxFuel`).
    pub fn from_opts_value(opts: Option<&JsonValue>) -> Self {
        let Some(opts) = opts.and_then(|v| v.as_object()) else {
            return Self::default();
        };

        let meta = opts.get("meta");
        let max_fuel = opts
            .get("max-fuel")
            .or_else(|| opts.get("max_fuel"))
            .or_else(|| opts.get("maxFuel"))
            .and_then(|v| v.as_u64());

        let track_all = matches!(meta, Some(JsonValue::Bool(true)));
        let meta_obj = meta.and_then(|v| v.as_object());

        let meta_flag = |k: &str| -> bool {
            meta_obj
                .and_then(|m| m.get(k))
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        };

        Self {
            track_time: track_all || meta_flag("time"),
            track_fuel: max_fuel.is_some() || track_all || meta_flag("fuel"),
            track_policy: track_all || meta_flag("policy"),
            max_fuel,
        }
    }

    #[inline]
    pub fn any_enabled(&self) -> bool {
        self.track_time || self.track_fuel || self.track_policy
    }

    /// Returns tracking options with all tracking enabled (time, fuel, policy).
    ///
    /// This is the default for "tracked" query endpoints where the user expects
    /// tracking information in the response.
    pub fn all_enabled() -> Self {
        Self {
            track_time: true,
            track_fuel: true,
            track_policy: true,
            max_fuel: None,
        }
    }
}

/// Policy execution statistics (Clojure parity: `{:executed N :allowed M}`)
#[derive(Debug, Clone, Default, Serialize)]
pub struct PolicyStats {
    pub executed: u64,
    pub allowed: u64,
}

/// Fuel limit exceeded (Clojure parity message: "Fuel limit exceeded")
#[derive(Debug, Clone, Error)]
#[error("Fuel limit exceeded")]
pub struct FuelExceededError {
    pub used: u64,
    pub limit: u64,
}

struct TrackerInner {
    // Time tracking
    start_time: Option<Instant>,

    // Fuel tracking
    fuel_total: AtomicU64,
    fuel_limit: u64, // 0 = unlimited

    // Policy tracking
    policy_stats: RwLock<HashMap<String, PolicyStats>>,

    options: TrackingOptions,
}

/// Execution tracker.
///
/// When disabled, this is a single `None` pointer (cheap to clone and pass around).
#[derive(Clone, Default)]
pub struct Tracker(Option<Arc<TrackerInner>>);

impl Tracker {
    /// Create a tracker from options. Returns a disabled tracker if no tracking is enabled.
    pub fn new(options: TrackingOptions) -> Self {
        if !options.any_enabled() {
            return Self(None);
        }

        Self(Some(Arc::new(TrackerInner {
            start_time: options.track_time.then(Instant::now),
            fuel_total: AtomicU64::new(0),
            fuel_limit: options.max_fuel.unwrap_or(0),
            policy_stats: RwLock::new(HashMap::new()),
            options,
        })))
    }

    /// Disabled tracker (zero overhead beyond a null check at call sites).
    #[inline]
    pub fn disabled() -> Self {
        Self(None)
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    #[inline]
    pub fn tracks_fuel(&self) -> bool {
        self.0
            .as_ref()
            .map(|i| i.options.track_fuel)
            .unwrap_or(false)
    }

    #[inline]
    pub fn tracks_policy(&self) -> bool {
        self.0
            .as_ref()
            .map(|i| i.options.track_policy)
            .unwrap_or(false)
    }

    /// Consume one unit of fuel (per emitted flake/item).
    ///
    /// Clojure parity: allows exactly `limit` items, errors when total becomes `limit + 1`.
    #[inline]
    pub fn consume_fuel_one(&self) -> Result<(), FuelExceededError> {
        let Some(inner) = &self.0 else {
            return Ok(());
        };
        if !inner.options.track_fuel {
            return Ok(());
        }

        let new_total = inner.fuel_total.fetch_add(1, Ordering::Relaxed) + 1;
        if inner.fuel_limit > 0 && new_total == inner.fuel_limit + 1 {
            return Err(FuelExceededError {
                used: new_total,
                limit: inner.fuel_limit,
            });
        }
        Ok(())
    }

    /// Record a policy evaluation attempt (increments for every policy considered).
    #[inline]
    pub fn policy_executed(&self, policy_id: &str) {
        let Some(inner) = &self.0 else {
            return;
        };
        if !inner.options.track_policy || policy_id.is_empty() {
            return;
        }

        if let Ok(mut stats) = inner.policy_stats.write() {
            stats.entry(policy_id.to_string()).or_default().executed += 1;
        }
    }

    /// Record a policy allow decision (only when that policy grants access).
    #[inline]
    pub fn policy_allowed(&self, policy_id: &str) {
        let Some(inner) = &self.0 else {
            return;
        };
        if !inner.options.track_policy || policy_id.is_empty() {
            return;
        }

        if let Ok(mut stats) = inner.policy_stats.write() {
            stats.entry(policy_id.to_string()).or_default().allowed += 1;
        }
    }

    /// Finalize tracking into a serializable tally.
    pub fn tally(&self) -> Option<TrackingTally> {
        let inner = self.0.as_ref()?;

        let elapsed = inner.start_time.map(|t| t.elapsed());
        Some(TrackingTally {
            time: elapsed.map(format_time_ms),
            time_ms: elapsed.map(|d| d.as_secs_f64() * 1000.0),
            fuel: inner
                .options
                .track_fuel
                .then(|| inner.fuel_total.load(Ordering::Relaxed)),
            policy: if inner.options.track_policy {
                inner.policy_stats.read().ok().map(|m| m.clone())
            } else {
                None
            },
        })
    }
}

/// Tracking tally returned on completion (Clojure parity keys).
#[derive(Debug, Clone, Serialize)]
pub struct TrackingTally {
    /// Formatted time string like `"12.34ms"`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Numeric milliseconds for span recording (not serialized to HTTP responses)
    #[serde(skip_serializing)]
    pub time_ms: Option<f64>,
    /// Total fuel consumed (just the number)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<u64>,
    /// Policy stats: `{policy-id -> {executed, allowed}}`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<HashMap<String, PolicyStats>>,
}

fn format_time_ms(duration: Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    format!("{:.2}ms", ms)
}
