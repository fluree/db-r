//! Shared types for the build pipeline.

use fluree_db_binary_index::{DictRefs, DictTreeRefs, InlineOrderRouting, NamedGraphRouting};
use fluree_db_core::ContentId;

/// Result of uploading index artifacts to CAS.
///
/// Separates default graph (g_id=0) from named graphs because the root format
/// inlines leaf routing for the default graph (no branch fetch needed) while
/// named graphs use branch CID pointers.
pub struct UploadedIndexes {
    /// Default graph (g_id=0): inline leaf routing per sort order.
    /// Leaves are uploaded to CAS; branch is NOT uploaded (routing is inline in root).
    pub default_graph_orders: Vec<InlineOrderRouting>,
    /// Named graphs (g_id!=0): branch CID per sort order per graph.
    /// Both branches and leaves are uploaded to CAS.
    pub named_graphs: Vec<NamedGraphRouting>,
}

/// Result of uploading persisted dict flat files to CAS.
///
/// Contains the CAS addresses for all dictionary artifacts plus derived metadata
/// needed for building the `IndexRootV5` (IRB1) root.
#[derive(Debug)]
pub struct UploadedDicts {
    pub dict_refs: DictRefs,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,
    /// Graph IRIs by dict_index (0-based). `g_id = dict_index + 1`.
    pub graph_iris: Vec<String>,
    /// Datatype IRIs by dt_id (0-based).
    pub datatype_iris: Vec<String>,
    /// Language tags by (lang_id - 1). `lang_id = index + 1`, 0 = "no tag".
    pub language_tags: Vec<String>,
}

/// Result of uploading an incrementally-updated reverse dictionary tree.
pub(crate) struct UpdatedReverseTree {
    pub(crate) tree_refs: DictTreeRefs,
    pub(crate) replaced_cids: Vec<ContentId>,
}
