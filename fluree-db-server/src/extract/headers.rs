//! Fluree-specific HTTP headers extractor

use axum::extract::FromRequestParts;
use axum::http::header::HeaderMap;
use axum::http::request::Parts;
use serde_json::Value as JsonValue;

use crate::error::{Result, ServerError};

/// Fluree-specific HTTP headers
///
/// These headers allow clients to specify query options, ledger selection,
/// identity, and policy without modifying the request body.
#[derive(Debug, Clone)]
pub struct FlureeHeaders {
    /// Raw HTTP headers (for telemetry/tracing)
    pub raw: HeaderMap,

    /// Ledger alias from header (lower precedence than path)
    pub ledger: Option<String>,

    /// Query identity (DID)
    pub identity: Option<String>,

    /// Policy document as JSON
    pub policy: Option<JsonValue>,

    /// Policy class IRI
    pub policy_class: Option<String>,

    /// Policy values as JSON
    pub policy_values: Option<JsonValue>,

    /// Enable all metadata tracking
    pub track_meta: bool,

    /// Track query fuel consumption
    pub track_fuel: bool,

    /// Track execution time
    pub track_time: bool,

    /// Maximum fuel limit
    pub max_fuel: Option<i64>,

    /// Content-Type header value
    pub content_type: Option<String>,
}

impl Default for FlureeHeaders {
    fn default() -> Self {
        Self {
            raw: HeaderMap::new(),
            ledger: None,
            identity: None,
            policy: None,
            policy_class: None,
            policy_values: None,
            track_meta: false,
            track_fuel: false,
            track_time: false,
            max_fuel: None,
            content_type: None,
        }
    }
}

impl FlureeHeaders {
    /// Header names
    pub const LEDGER: &'static str = "fluree-ledger";
    pub const IDENTITY: &'static str = "fluree-identity";
    pub const POLICY: &'static str = "fluree-policy";
    pub const POLICY_CLASS: &'static str = "fluree-policy-class";
    pub const POLICY_VALUES: &'static str = "fluree-policy-values";
    pub const TRACK_META: &'static str = "fluree-track-meta";
    pub const TRACK_FUEL: &'static str = "fluree-track-fuel";
    pub const TRACK_TIME: &'static str = "fluree-track-time";
    pub const MAX_FUEL: &'static str = "fluree-max-fuel";

    /// Parse headers from a HeaderMap
    pub fn from_headers(headers: &HeaderMap) -> Result<Self> {
        let mut fluree_headers = Self::default();
        fluree_headers.raw = headers.clone();

        // String headers
        if let Some(val) = get_header_str(headers, Self::LEDGER) {
            fluree_headers.ledger = Some(val.to_string());
        }

        if let Some(val) = get_header_str(headers, Self::IDENTITY) {
            fluree_headers.identity = Some(val.to_string());
        }

        if let Some(val) = get_header_str(headers, Self::POLICY_CLASS) {
            fluree_headers.policy_class = Some(val.to_string());
        }

        // JSON headers
        if let Some(val) = get_header_str(headers, Self::POLICY) {
            fluree_headers.policy = Some(serde_json::from_str(val).map_err(|e| {
                ServerError::invalid_header(format!("{} is not valid JSON: {}", Self::POLICY, e))
            })?);
        }

        if let Some(val) = get_header_str(headers, Self::POLICY_VALUES) {
            fluree_headers.policy_values = Some(serde_json::from_str(val).map_err(|e| {
                ServerError::invalid_header(format!(
                    "{} is not valid JSON: {}",
                    Self::POLICY_VALUES,
                    e
                ))
            })?);
        }

        // Boolean headers (presence or "true" value)
        fluree_headers.track_meta = is_header_truthy(headers, Self::TRACK_META);
        fluree_headers.track_fuel =
            fluree_headers.track_meta || is_header_truthy(headers, Self::TRACK_FUEL);
        fluree_headers.track_time =
            fluree_headers.track_meta || is_header_truthy(headers, Self::TRACK_TIME);

        // Numeric headers
        if let Some(val) = get_header_str(headers, Self::MAX_FUEL) {
            fluree_headers.max_fuel = Some(val.parse().map_err(|_| {
                ServerError::invalid_header(format!("{} must be a number", Self::MAX_FUEL))
            })?);
        }

        // Content-Type
        if let Some(ct) = headers.get(axum::http::header::CONTENT_TYPE) {
            if let Ok(ct_str) = ct.to_str() {
                fluree_headers.content_type = Some(ct_str.to_string());
            }
        }

        Ok(fluree_headers)
    }

    /// Check if tracking is enabled (any tracking header)
    pub fn has_tracking(&self) -> bool {
        self.track_meta || self.track_fuel || self.track_time
    }

    /// Check if this is a SPARQL query based on Content-Type
    pub fn is_sparql_query(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/sparql-query"))
            .unwrap_or(false)
    }

    /// Check if this is a SPARQL update based on Content-Type
    pub fn is_sparql_update(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/sparql-update"))
            .unwrap_or(false)
    }

    /// Check if this is a JWT/JWS based on Content-Type
    pub fn is_jwt(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/jwt"))
            .unwrap_or(false)
    }

    /// Convert policy_values to a HashMap for credential API
    ///
    /// Returns None if no policy values are set, or an error if they're not a valid JSON object.
    pub fn policy_values_map(
        &self,
    ) -> Result<Option<std::collections::HashMap<String, JsonValue>>> {
        match &self.policy_values {
            None => Ok(None),
            Some(JsonValue::Object(obj)) => {
                let map: std::collections::HashMap<String, JsonValue> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Ok(Some(map))
            }
            Some(_) => Err(ServerError::invalid_header(
                "policy-values must be a JSON object",
            )),
        }
    }

    /// Inject header values into query opts JSON
    ///
    /// Header values serve as defaults - they don't override explicit body values.
    pub fn inject_into_opts(&self, opts: &mut serde_json::Map<String, JsonValue>) {
        // Only inject if not already present in opts
        if self.identity.is_some() && !opts.contains_key("identity") {
            opts.insert(
                "identity".to_string(),
                JsonValue::String(self.identity.clone().unwrap()),
            );
        }

        if self.policy.is_some() && !opts.contains_key("policy") {
            opts.insert("policy".to_string(), self.policy.clone().unwrap());
        }

        if self.policy_class.is_some() && !opts.contains_key("policy-class") {
            opts.insert(
                "policy-class".to_string(),
                JsonValue::String(self.policy_class.clone().unwrap()),
            );
        }

        if self.policy_values.is_some() && !opts.contains_key("policy-values") {
            opts.insert(
                "policy-values".to_string(),
                self.policy_values.clone().unwrap(),
            );
        }

        if self.max_fuel.is_some() && !opts.contains_key("max-fuel") {
            opts.insert(
                "max-fuel".to_string(),
                JsonValue::Number(self.max_fuel.unwrap().into()),
            );
        }

        // Inject tracking options into meta if any tracking is enabled via headers
        // (only if meta is not already present in opts)
        if self.has_tracking() && !opts.contains_key("meta") {
            if self.track_meta {
                // track-meta enables all tracking
                opts.insert("meta".to_string(), JsonValue::Bool(true));
            } else {
                // Selective tracking via individual flags
                let mut meta = serde_json::Map::new();
                if self.track_time {
                    meta.insert("time".to_string(), JsonValue::Bool(true));
                }
                if self.track_fuel {
                    meta.insert("fuel".to_string(), JsonValue::Bool(true));
                }
                if !meta.is_empty() {
                    opts.insert("meta".to_string(), JsonValue::Object(meta));
                }
            }
        }
    }
}

/// Get a header value as a string slice
fn get_header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Check if a header is present and truthy
fn is_header_truthy(headers: &HeaderMap, name: &str) -> bool {
    match get_header_str(headers, name) {
        Some(v) => v.eq_ignore_ascii_case("true") || v == "1" || v.is_empty(),
        None => false,
    }
}

/// Axum extractor implementation
#[axum::async_trait]
impl<S> FromRequestParts<S> for FlureeHeaders
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        FlureeHeaders::from_headers(&parts.headers)
    }
}
