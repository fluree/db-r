pub mod branch;
pub mod error;
pub mod file_sync;
pub mod format;
pub mod id;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod recall;
pub mod schema;
pub mod secrets;
pub mod store;
pub mod turtle_io;
pub mod types;
pub mod vocab;

pub use branch::detect_git_branch;
pub use error::{MemoryError, Result};
pub use format::{
    format_context, format_explain, format_json, format_recall_json, format_recall_text,
    format_status_text, format_text,
};
pub use id::generate_memory_id;
#[cfg(feature = "mcp")]
pub use mcp::MemoryToolService;
pub use recall::RecallEngine;
pub use secrets::SecretDetector;
pub use store::MemoryStore;
pub use types::{
    Memory, MemoryFilter, MemoryInput, MemoryKind, MemoryStatus, MemoryUpdate, RecallResult, Scope,
    ScoredMemory, Sensitivity, Severity,
};
