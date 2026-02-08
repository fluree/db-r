//! Auth management commands: status, login, logout
//!
//! Manages bearer tokens stored in remote configs. Tokens are stored
//! in `.fluree/config.toml` as part of the remote's `auth` section.
//!
//! Token values are never printed to stdout — `status` shows presence,
//! expiry, and identity only.
//!
//! For OIDC remotes (`auth.type = "oidc_device"`), `login` runs the
//! OAuth 2.0 Device Authorization Grant, then exchanges the IdP token
//! for a Fluree-scoped Bearer token via the configured exchange endpoint.

use crate::cli::AuthAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{RemoteAuthType, RemoteEndpoint, SyncConfigStore};
use std::io::{self, Read};
use std::path::Path;

pub async fn run(action: AuthAction, fluree_dir: &Path) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());

    match action {
        AuthAction::Status { remote } => run_status(&store, remote.as_deref()).await,
        AuthAction::Login { remote, token } => run_login(&store, remote.as_deref(), token).await,
        AuthAction::Logout { remote } => run_logout(&store, remote.as_deref()).await,
    }
}

/// Resolve which remote to use: explicit name, or the only configured remote.
async fn resolve_remote_name(
    store: &TomlSyncConfigStore,
    explicit: Option<&str>,
) -> CliResult<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    match remotes.len() {
        0 => Err(CliError::Config(
            "no remotes configured. Use `fluree remote add <name> <url>` first.".to_string(),
        )),
        1 => Ok(remotes[0].name.as_str().to_string()),
        _ => Err(CliError::Usage(
            "multiple remotes configured; specify one with --remote <name>".to_string(),
        )),
    }
}

// =============================================================================
// Status
// =============================================================================

async fn run_status(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    println!("{}", "Auth Status:".bold());
    println!("  Remote: {}", name.green());

    // Show auth type
    match config.auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            println!("  Auth type: {}", "oidc_device".cyan());
            if let Some(ref issuer) = config.auth.issuer {
                println!("  Issuer: {}", issuer);
            }
        }
        Some(RemoteAuthType::Token) => {
            println!("  Auth type: token");
        }
        None => {}
    }

    match &config.auth.token {
        Some(token) => {
            println!("  Token:  {}", "configured".green());

            // Decode token claims without verification to show expiry/identity
            match decode_token_summary(token) {
                Ok(summary) => {
                    if let Some(exp) = summary.expiry {
                        println!("  Expiry: {}", exp);
                    } else {
                        println!("  Expiry: {}", "no expiry claim".yellow());
                    }
                    if let Some(identity) = &summary.identity {
                        println!("  Identity: {}", identity);
                    }
                    if let Some(issuer) = &summary.issuer {
                        println!("  Issuer: {}", issuer);
                    }
                    if let Some(subject) = &summary.subject {
                        println!("  Subject: {}", subject);
                    }
                }
                Err(_) => {
                    println!("  {}", "(could not decode token claims)".yellow());
                }
            }

            if config.auth.refresh_token.is_some() {
                println!("  Refresh: available");
            }
        }
        None => {
            println!("  Token:  {}", "not configured".yellow());
            println!(
                "  {} fluree auth login --remote {}",
                "hint:".cyan().bold(),
                name
            );
        }
    }

    Ok(())
}

// =============================================================================
// Login
// =============================================================================

async fn run_login(
    store: &TomlSyncConfigStore,
    remote: Option<&str>,
    token_arg: Option<String>,
) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    // Route based on auth type
    let is_oidc = config.auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice);

    if is_oidc && token_arg.is_none() {
        // OIDC device code flow
        run_oidc_login(&mut config).await?;
    } else if config.auth.auth_type.is_none() && token_arg.is_none() {
        // Auth type unset and no explicit token — try discovery first
        if try_discover_and_login(&mut config).await? {
            // Discovery succeeded and OIDC login completed
        } else {
            // No discovery available — fall back to manual token prompt
            let token = read_token(token_arg)?;
            config.auth.token = Some(token);
        }
    } else {
        // Manual token flow (explicit token or token auth type)
        let token = read_token(token_arg)?;
        config.auth.token = Some(token);
    }

    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token stored for remote '{}'", name.green());

    // Show a brief summary of what was stored
    if let Some(ref tok) = config.auth.token {
        if let Ok(summary) = decode_token_summary(tok) {
            if let Some(exp) = summary.expiry {
                println!("  Expiry: {}", exp);
            }
            if let Some(identity) = &summary.identity {
                println!("  Identity: {}", identity);
            }
        }
    }

    Ok(())
}

/// Read a token from argument, file, stdin, or interactive prompt.
fn read_token(token_arg: Option<String>) -> CliResult<String> {
    let token = match token_arg {
        Some(t) if t == "@-" => {
            let mut buf = String::new();
            io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token from stdin: {e}")))?;
            buf.trim().to_string()
        }
        Some(t) if t.starts_with('@') => {
            let path = t.strip_prefix('@').unwrap();
            let expanded = shellexpand::tilde(path);
            std::fs::read_to_string(expanded.as_ref())
                .map_err(|e| CliError::Input(format!("failed to read token file: {e}")))?
                .trim()
                .to_string()
        }
        Some(t) => t,
        None => {
            eprintln!("Paste token (then press Enter):");
            let mut buf = String::new();
            io::stdin()
                .read_line(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token: {e}")))?;
            buf.trim().to_string()
        }
    };

    if token.is_empty() {
        return Err(CliError::Input("token cannot be empty".to_string()));
    }

    Ok(token)
}

// =============================================================================
// OIDC Discovery Fallback
// =============================================================================

/// When auth type is unset, try `/.well-known/fluree.json` discovery.
/// If OIDC config is found, configure the remote and run the device flow.
/// Returns `true` if OIDC login was performed, `false` if no discovery available.
async fn try_discover_and_login(
    config: &mut fluree_db_nameservice_sync::RemoteConfig,
) -> CliResult<bool> {
    let base_url = match &config.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => return Ok(false),
    };

    eprintln!("No auth type configured; checking server for OIDC discovery...");

    match super::remote::discover_auth(&base_url).await {
        Ok(Some(discovered_auth)) => {
            if discovered_auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice) {
                // Apply discovered OIDC config
                config.auth = discovered_auth;
                eprintln!(
                    "  {} auto-discovered OIDC auth from server",
                    "info:".cyan().bold()
                );

                // Run the OIDC device flow
                run_oidc_login(config).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Ok(None) => {
            eprintln!("  No OIDC discovery available; using manual token login.");
            Ok(false)
        }
        Err(msg) => {
            eprintln!("  {} discovery failed: {}", "warn:".yellow().bold(), msg);
            Ok(false)
        }
    }
}

// =============================================================================
// OIDC Device Code Flow
// =============================================================================

/// Run the full OIDC device authorization grant + token exchange.
///
/// 1. Discover OIDC endpoints from `{issuer}/.well-known/openid-configuration`
/// 2. POST to `device_authorization_endpoint` to get device/user codes
/// 3. Open browser (or print URL) for user to authenticate
/// 4. Poll `token_endpoint` until complete
/// 5. Exchange IdP token for Fluree Bearer token at `exchange_url`
/// 6. Store both tokens in the config
async fn run_oidc_login(config: &mut fluree_db_nameservice_sync::RemoteConfig) -> CliResult<()> {
    let issuer = config
        .auth
        .issuer
        .as_ref()
        .ok_or_else(|| CliError::Config("OIDC auth configured but 'issuer' is missing".into()))?
        .clone();

    let client_id = config
        .auth
        .client_id
        .as_ref()
        .ok_or_else(|| CliError::Config("OIDC auth configured but 'client_id' is missing".into()))?
        .clone();

    let exchange_url = config
        .auth
        .exchange_url
        .as_ref()
        .ok_or_else(|| {
            CliError::Config("OIDC auth configured but 'exchange_url' is missing".into())
        })?
        .clone();

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| CliError::Remote(format!("failed to build HTTP client: {e}")))?;

    // Step 1: Discover OIDC endpoints
    eprintln!("Discovering OIDC endpoints...");
    let oidc_config = discover_oidc_endpoints(&http, &issuer).await?;

    // Step 2: Request device code
    eprintln!("Requesting device authorization...");
    let device_resp = request_device_code(
        &http,
        &oidc_config.device_authorization_endpoint,
        &client_id,
    )
    .await?;

    // Step 3: Show user code and open browser
    eprintln!();
    eprintln!("  {} Open this URL and enter the code below:", ">>".bold());
    eprintln!("  URL:  {}", device_resp.verification_uri.cyan());
    if let Some(ref complete_uri) = device_resp.verification_uri_complete {
        eprintln!("  (or)  {}", complete_uri.cyan());
    }
    eprintln!("  Code: {}", device_resp.user_code.bold().green());
    eprintln!();

    // Try to open browser (non-fatal if it fails)
    let open_url = device_resp
        .verification_uri_complete
        .as_deref()
        .unwrap_or(&device_resp.verification_uri);
    if open::that(open_url).is_ok() {
        eprintln!("  (browser opened automatically)");
    }

    // Step 4: Poll for token
    eprintln!("Waiting for authorization...");
    let idp_tokens = poll_for_token(
        &http,
        &oidc_config.token_endpoint,
        &device_resp.device_code,
        &client_id,
        device_resp.interval,
    )
    .await?;

    eprintln!("  IdP authentication successful");

    // Step 5: Exchange IdP token for Fluree token
    eprintln!("Exchanging for Fluree token...");
    let fluree_tokens = exchange_token(&http, &exchange_url, &idp_tokens.access_token).await?;

    // Step 6: Store tokens
    config.auth.token = Some(fluree_tokens.access_token);
    config.auth.refresh_token = fluree_tokens.refresh_token;

    Ok(())
}

/// Minimal OIDC discovery response (only fields we need).
struct OidcDiscovery {
    device_authorization_endpoint: String,
    token_endpoint: String,
}

/// Fetch OIDC discovery document.
async fn discover_oidc_endpoints(http: &reqwest::Client, issuer: &str) -> CliResult<OidcDiscovery> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );

    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("OIDC discovery failed: {e}")))?;

    if !resp.status().is_success() {
        return Err(CliError::Remote(format!(
            "OIDC discovery returned {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("OIDC discovery invalid JSON: {e}")))?;

    let device_authorization_endpoint = body
        .get("device_authorization_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CliError::Remote(
                "OIDC provider does not support device authorization grant \
                 (missing device_authorization_endpoint)"
                    .into(),
            )
        })?
        .to_string();

    let token_endpoint = body
        .get("token_endpoint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("OIDC discovery missing token_endpoint".into()))?
        .to_string();

    Ok(OidcDiscovery {
        device_authorization_endpoint,
        token_endpoint,
    })
}

/// Device authorization response.
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    interval: u64,
}

/// Request a device code from the IdP.
async fn request_device_code(
    http: &reqwest::Client,
    device_auth_endpoint: &str,
    client_id: &str,
) -> CliResult<DeviceAuthResponse> {
    let resp = http
        .post(device_auth_endpoint)
        .form(&[("client_id", client_id), ("scope", "openid")])
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("device authorization request failed: {e}")))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Remote(format!(
            "device authorization failed: {body}"
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("device authorization invalid JSON: {e}")))?;

    let device_code = body
        .get("device_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing device_code".into()))?
        .to_string();

    let user_code = body
        .get("user_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing user_code".into()))?
        .to_string();

    let verification_uri = body
        .get("verification_uri")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("device response missing verification_uri".into()))?
        .to_string();

    let verification_uri_complete = body
        .get("verification_uri_complete")
        .and_then(|v| v.as_str())
        .map(String::from);

    let interval = body.get("interval").and_then(|v| v.as_u64()).unwrap_or(5);

    Ok(DeviceAuthResponse {
        device_code,
        user_code,
        verification_uri,
        verification_uri_complete,
        interval,
    })
}

/// IdP token response (from device code polling).
struct IdpTokenResponse {
    access_token: String,
}

/// Poll the token endpoint until the user completes authorization.
async fn poll_for_token(
    http: &reqwest::Client,
    token_endpoint: &str,
    device_code: &str,
    client_id: &str,
    interval_secs: u64,
) -> CliResult<IdpTokenResponse> {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));

    loop {
        tokio::time::sleep(interval).await;

        let resp = http
            .post(token_endpoint)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", device_code),
                ("client_id", client_id),
            ])
            .send()
            .await
            .map_err(|e| CliError::Remote(format!("token poll failed: {e}")))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| CliError::Remote(format!("token poll invalid JSON: {e}")))?;

        if status.is_success() {
            let access_token = body
                .get("access_token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CliError::Remote("token response missing access_token".into()))?
                .to_string();

            return Ok(IdpTokenResponse { access_token });
        }

        // Check error type
        let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("");

        match error {
            "authorization_pending" => {
                // User hasn't completed auth yet — keep polling
                eprint!(".");
                continue;
            }
            "slow_down" => {
                // Back off
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            "expired_token" => {
                return Err(CliError::Remote(
                    "device code expired. Run `fluree auth login` to try again.".into(),
                ));
            }
            "access_denied" => {
                return Err(CliError::Remote(
                    "authorization denied by user or IdP".into(),
                ));
            }
            _ => {
                let desc = body
                    .get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                return Err(CliError::Remote(format!(
                    "device code grant failed: {error}: {desc}"
                )));
            }
        }
    }
}

/// Fluree token exchange response.
struct ExchangeResponse {
    access_token: String,
    refresh_token: Option<String>,
}

/// Exchange an IdP access token for a Fluree-scoped Bearer token.
async fn exchange_token(
    http: &reqwest::Client,
    exchange_url: &str,
    idp_token: &str,
) -> CliResult<ExchangeResponse> {
    let body = serde_json::json!({
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": idp_token,
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token"
    });

    let resp = http
        .post(exchange_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| CliError::Remote(format!("token exchange failed: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let err_body: serde_json::Value = resp.json().await.unwrap_or_default();
        let desc = err_body
            .get("error_description")
            .and_then(|v| v.as_str())
            .or_else(|| err_body.get("error").and_then(|v| v.as_str()))
            .unwrap_or("exchange rejected");
        return Err(CliError::Remote(format!(
            "token exchange failed ({status}): {desc}"
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Remote(format!("exchange response invalid JSON: {e}")))?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("exchange response missing access_token".into()))?
        .to_string();

    let refresh_token = body
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(ExchangeResponse {
        access_token,
        refresh_token,
    })
}

// =============================================================================
// Logout
// =============================================================================

async fn run_logout(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    if config.auth.token.is_none() && config.auth.refresh_token.is_none() {
        println!("No token stored for remote '{}'", name);
        return Ok(());
    }

    config.auth.token = None;
    config.auth.refresh_token = None;
    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token cleared for remote '{}'", name.green());
    Ok(())
}

// =============================================================================
// Token summary decoding (no verification)
// =============================================================================

struct TokenSummary {
    expiry: Option<String>,
    identity: Option<String>,
    issuer: Option<String>,
    subject: Option<String>,
}

/// Decode a JWT/JWS payload without verification to extract summary info.
fn decode_token_summary(token: &str) -> Result<TokenSummary, ()> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(());
    }

    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| ())?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).map_err(|_| ())?;

    let expiry = claims.get("exp").and_then(|v| v.as_u64()).map(|exp| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if exp < now {
            format!("{} (expired)", format_timestamp(exp))
                .red()
                .to_string()
        } else {
            format_timestamp(exp)
        }
    });

    let identity = claims
        .get("fluree.identity")
        .and_then(|v| v.as_str())
        .map(String::from);

    let issuer = claims.get("iss").and_then(|v| v.as_str()).map(String::from);

    let subject = claims.get("sub").and_then(|v| v.as_str()).map(String::from);

    Ok(TokenSummary {
        expiry,
        identity,
        issuer,
        subject,
    })
}

/// Format a unix timestamp as a human-readable string.
fn format_timestamp(ts: u64) -> String {
    let secs = ts;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;

    let mut y = 1970i64;
    let mut remaining = days_since_epoch as i64;
    loop {
        let days_in_year = if is_leap_year(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let mut m = 1u32;
    let days_in_month = [
        31,
        28 + i64::from(is_leap_year(y)),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for &dim in &days_in_month {
        if remaining < dim {
            break;
        }
        remaining -= dim;
        m += 1;
    }
    let d = remaining + 1;

    format!("{y:04}-{m:02}-{d:02} {hours:02}:{minutes:02} UTC")
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
