//! CoW B-tree dictionary storage for subject and string dictionaries.
//!
//! Replaces the flat forward/index/reverse file format with 4 CoW trees:
//!
//! - **Subject forward**: `sid64 → suffix` (sorted by sid64, ns-compressed)
//! - **Subject reverse**: `[ns_code BE][suffix] → sid64` (sorted lexicographically, ns-compressed)
//! - **String forward**: `string_id → value` (sorted by string_id)
//! - **String reverse**: `value → string_id` (sorted by value)
//!
//! Each tree is a single-level structure: one branch manifest pointing to
//! multiple leaf files, all content-addressed for CAS storage.
//!
//! ## Leaf Format
//!
//! Leaves use an offset-table design for O(log n) binary search on
//! variable-length entries. Each leaf is a self-contained blob:
//!
//! ```text
//! [magic: 4B][entry_count: u32][offset_table: u32 × entry_count][entries...]
//! ```
//!
//! The offset table stores the byte offset of each entry relative to the
//! start of the entries section, enabling random access for binary search.
//!
//! ## Branch Format
//!
//! Branches map key ranges to leaf CAS addresses:
//!
//! ```text
//! [magic: 4B][leaf_count: u32][offset_table: u32 × leaf_count][leaf_entries...]
//! ```

pub mod varint;
pub mod forward_leaf;
pub mod reverse_leaf;
pub mod branch;
pub mod builder;
pub mod reader;

pub use branch::DictBranch;
pub use builder::TreeBuildResult;
pub use reader::DictTreeReader;
