//! DynamoDB table schema constants
//!
//! Defines the attribute names and values used in the fluree-nameservice table.
//!
//! ## Table Schema
//!
//! ```text
//! Table: fluree-nameservice (configurable)
//!
//! Primary Key:
//!   - ledger_alias (String, Partition Key): e.g., "mydb:main"
//!
//! Attributes:
//!   - ledger_alias: String (PK) - canonical key including branch
//!   - ledger_name: String - ledger name without branch
//!   - branch: String - branch name
//!   - commit_address: String (optional) - latest commit storage address
//!   - commit_t: Number - transaction time of latest commit
//!   - index_address: String (optional) - latest index storage address
//!   - index_t: Number - transaction time of latest index
//!   - default_context_address: String (optional) - JSON-LD context address
//!   - status: String ("ready" | "retracted")
//!   - updated_at: Number (Unix epoch seconds)
//!
//! V2 Extension Attributes:
//!   - status_v: Number - status watermark (monotonically increasing)
//!   - status_meta: Map (optional) - extensible status metadata
//!   - config_v: Number - config watermark (monotonically increasing)
//!   - config_meta: Map (optional) - extensible config metadata
//! ```

/// Primary key attribute - ledger alias including branch (e.g., "mydb:main")
pub const ATTR_LEDGER_ALIAS: &str = "ledger_alias";

/// Ledger name without branch (e.g., "mydb")
pub const ATTR_LEDGER_NAME: &str = "ledger_name";

/// Branch name (e.g., "main")
pub const ATTR_BRANCH: &str = "branch";

/// Latest commit storage address
pub const ATTR_COMMIT_ADDRESS: &str = "commit_address";

/// Transaction time of latest commit
pub const ATTR_COMMIT_T: &str = "commit_t";

/// Latest index storage address
pub const ATTR_INDEX_ADDRESS: &str = "index_address";

/// Transaction time of latest index
pub const ATTR_INDEX_T: &str = "index_t";

/// Default JSON-LD context storage address
pub const ATTR_DEFAULT_CONTEXT_ADDRESS: &str = "default_context_address";

/// Status attribute ("ready" or "retracted")
/// Note: "status" is a DynamoDB reserved word, use ExpressionAttributeNames
pub const ATTR_STATUS: &str = "status";

/// Last update timestamp (Unix epoch seconds)
pub const ATTR_UPDATED_AT: &str = "updated_at";

/// Status value: ledger is active
pub const STATUS_READY: &str = "ready";

/// Status value: ledger has been retracted
pub const STATUS_RETRACTED: &str = "retracted";

/// Default table name
pub const DEFAULT_TABLE_NAME: &str = "fluree-nameservice";

// ---------------------------------------------------------------------------
// V2 Extension Attributes (Status and Config concerns)
// ---------------------------------------------------------------------------

/// Status watermark (v2) - monotonically increasing version counter
/// Defaults to 1 if missing (for legacy records)
pub const ATTR_STATUS_V: &str = "status_v";

/// Status metadata (v2) - extensible metadata map (queue_depth, locks, etc.)
pub const ATTR_STATUS_META: &str = "status_meta";

/// Config watermark (v2) - monotonically increasing version counter
/// Defaults to 0 (unborn) if missing, or 1 if default_context_address exists
pub const ATTR_CONFIG_V: &str = "config_v";

/// Config metadata (v2) - extensible config settings map
pub const ATTR_CONFIG_META: &str = "config_meta";
