//! HTTP route handlers and router configuration

mod admin;
mod admin_auth;
mod events;
mod ledger;
mod nameservice_refs;
mod query;
mod storage_proxy;
mod stubs;
mod transact;

use crate::state::AppState;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Build the main application router
pub fn build_router(state: Arc<AppState>) -> Router {
    // Admin-protected routes (create, drop) - require admin token when configured
    let admin_protected_routes = Router::new()
        .route("/fluree/create", post(ledger::create))
        .route("/fluree/drop", post(ledger::drop))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth::require_admin_token,
        ))
        .with_state(state.clone());

    let mut router = Router::new()
        // Health check
        .route("/health", get(admin::health))
        // Admin endpoints (stats is read-only, no auth required)
        .route("/fluree/stats", get(admin::stats))
        // Ledger management (read-only)
        .route("/fluree/ledger-info", get(ledger::info))
        .route("/fluree/exists", get(ledger::exists))
        // Merge admin-protected routes
        .merge(admin_protected_routes)
        // Query endpoints
        .route("/fluree/query", get(query::query).post(query::query))
        .route("/fluree/explain", get(query::explain).post(query::explain))
        // Transaction endpoints
        // /fluree/update is the primary endpoint, /fluree/transact is legacy alias
        .route("/fluree/update", post(transact::transact))
        .route("/fluree/transact", post(transact::transact))
        .route("/fluree/insert", post(transact::insert))
        .route("/fluree/upsert", post(transact::upsert))
        // SSE event streaming
        .route("/fluree/events", get(events::events))
        // Storage proxy endpoints (for peer mode)
        .route(
            "/fluree/storage/ns/:alias",
            get(storage_proxy::get_ns_record),
        )
        .route("/fluree/storage/block", post(storage_proxy::get_block))
        // Nameservice ref endpoints (for remote sync)
        .route(
            "/fluree/nameservice/refs/:alias/commit",
            post(nameservice_refs::push_commit_ref),
        )
        .route(
            "/fluree/nameservice/refs/:alias/index",
            post(nameservice_refs::push_index_ref),
        )
        .route(
            "/fluree/nameservice/refs/:alias/init",
            post(nameservice_refs::init_ledger),
        )
        .route(
            "/fluree/nameservice/snapshot",
            get(nameservice_refs::snapshot),
        )
        // Stub endpoints (not yet implemented)
        .route("/fluree/subscribe", get(stubs::subscribe))
        .route("/fluree/remote/:path", get(stubs::remote).post(stubs::remote))
        // Dynamic ledger routes (/{ledger}/query, /{ledger}/transact, etc.)
        .route("/:ledger/query", get(query::query_ledger).post(query::query_ledger))
        .route("/:ledger/update", post(transact::transact_ledger))
        .route("/:ledger/transact", post(transact::transact_ledger))
        .route("/:ledger/insert", post(transact::insert_ledger))
        .route("/:ledger/upsert", post(transact::upsert_ledger))
        // OpenAPI spec
        .route("/swagger.json", get(admin::openapi_spec));

    // Add MCP router if enabled
    if state.config.mcp_enabled {
        let mcp_router = crate::mcp::build_mcp_router(state.clone());
        router = router.nest("/mcp", mcp_router);
    }

    // Add state
    let mut router = router.with_state(state.clone());

    // Add middleware
    router = router.layer(TraceLayer::new_for_http());

    // Add CORS if enabled
    if state.config.cors_enabled {
        router = router.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    router
}
