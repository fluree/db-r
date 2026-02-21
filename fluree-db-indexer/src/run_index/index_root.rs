//! Binary index root types and IRB1 (v5) wire format.
//!
//! CID reference types (`DictRefs`, `GraphOrderRefs`, etc.) are used as
//! intermediates in the index build pipeline. `IndexRootV5` is the canonical
//! binary root descriptor published to the nameservice via `index_head_id`.
//!
//! All artifact references use `ContentId` (CIDv1) values.

use fluree_db_core::index_schema::IndexSchema;
use fluree_db_core::index_stats::IndexStats;
use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// CID reference types
// ============================================================================

/// CID references for a dictionary CoW tree (branch + leaves).
///
/// Mirrors `GraphOrderRefs` — a branch manifest that references
/// a set of leaf blobs. The branch holds the key-range index; leaves
/// hold the actual dictionary entries.
#[derive(Debug, Clone, PartialEq)]
pub struct DictTreeRefs {
    /// CID of the branch manifest (DTB1).
    pub branch: ContentId,
    /// CIDs of leaf blobs, ordered by leaf index.
    pub leaves: Vec<ContentId>,
}

/// CID references for a per-predicate vector arena (manifest + shards).
///
/// Stored explicitly so GC can reach all shard CIDs without
/// parsing manifests during retention walks.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorDictRef {
    /// CID of the manifest JSON (VAM1).
    pub manifest: ContentId,
    /// CIDs of all shard blobs (VAS1), ordered by shard index.
    pub shards: Vec<ContentId>,
}

/// CID references for all dictionary artifacts stored in CAS.
///
/// Forward dictionaries use FPK1 packs (DictPackRefs).
/// Reverse dictionaries use CoW trees (DictTreeRefs).
/// Small dictionaries (graphs, datatypes, languages) are embedded
/// inline in the index root and therefore do not appear here.
#[derive(Debug, Clone, PartialEq)]
pub struct DictRefs {
    /// Forward dictionary packs (string + subject, FPK1 format).
    pub forward_packs: DictPackRefs,
    /// Subject reverse tree: [ns_code BE][suffix] → sid64 (ns-compressed).
    pub subject_reverse: DictTreeRefs,
    /// String reverse tree: value → string_id.
    pub string_reverse: DictTreeRefs,
    /// Per-predicate numbig arenas. Key is `p_id` as string.
    pub numbig: BTreeMap<String, ContentId>,
    /// Per-predicate vector arena metadata. Key is `p_id` as string.
    /// Value contains manifest CID + all shard CIDs.
    pub vectors: BTreeMap<String, VectorDictRef>,
}

// ============================================================================
// Forward dict pack types (FPK1)
// ============================================================================

/// A single entry in the pack branch routing table.
///
/// Maps an ID range to a pack CID. Used inline in the index root
/// to route forward dictionary lookups without an extra fetch.
#[derive(Debug, Clone, PartialEq)]
pub struct PackBranchEntry {
    pub first_id: u64,
    pub last_id: u64,
    pub pack_cid: ContentId,
}

/// Forward dictionary pack references (replaces tree-based forward dicts).
///
/// String forward packs are a flat list (global contiguous IDs).
/// Subject forward packs are grouped by namespace code (contiguous local IDs within each ns).
#[derive(Debug, Clone, PartialEq)]
pub struct DictPackRefs {
    /// String forward packs, sorted by first_id.
    pub string_fwd_packs: Vec<PackBranchEntry>,
    /// Subject forward packs, grouped by ns_code. Sorted by ns_code, then by first_id within.
    pub subject_fwd_ns_packs: Vec<(u16, Vec<PackBranchEntry>)>,
}

/// CID references for a single graph + sort order (one branch + its leaves).
#[derive(Debug, Clone, PartialEq)]
pub struct GraphOrderRefs {
    /// CID of the branch manifest (FBR1).
    pub branch: ContentId,
    /// CIDs of leaf files (FLI2), ordered by leaf index.
    pub leaves: Vec<ContentId>,
}

/// CID references for all sort orders within a single graph.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphRefs {
    pub g_id: GraphId,
    /// Order name (e.g. `"spot"`) → branch + leaves CIDs.
    pub orders: BTreeMap<String, GraphOrderRefs>,
}

// ============================================================================
// GC chain types (prev_index / garbage)
// ============================================================================

/// Reference to the previous index root in the GC chain.
///
/// The garbage collector walks this chain backwards to determine which roots
/// (and their associated CAS artifacts) are eligible for deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryPrevIndexRef {
    /// `index_t` of the previous root.
    pub t: i64,
    /// CID of the previous root blob.
    pub id: ContentId,
}

/// Reference to this root's garbage manifest.
///
/// The garbage manifest lists CIDs that were replaced when building
/// this root from the previous one. The GC collector reads this to know which
/// objects to delete when this root ages out of the retention window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryGarbageRef {
    /// CID of the garbage record JSON blob.
    pub id: ContentId,
}

// ============================================================================
// IndexRootV5 (binary, IRB1)
// ============================================================================

use super::branch::LeafEntry;
use super::run_record::{RunSortOrder, RECORD_WIRE_SIZE};
use super::stats_wire;
use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT;
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;

/// Magic bytes for the binary index root (`IRB1`).
const ROOT_V5_MAGIC: [u8; 4] = *b"IRB1";

/// Wire format version for IRB1.
/// v2: forward dicts use FPK1 packs instead of DLF1 trees.
const ROOT_V5_VERSION: u8 = 2;

/// Fixed header size: magic(4) + version(1) + flags(1) + pad(2) + index_t(8) + base_t(8) = 24.
const ROOT_V5_HEADER_LEN: usize = 24;

/// Flag bit: has_stats section.
const FLAG_HAS_STATS: u8 = 1 << 0;
/// Flag bit: has_schema section.
const FLAG_HAS_SCHEMA: u8 = 1 << 1;
/// Flag bit: has_prev_index.
const FLAG_HAS_PREV_INDEX: u8 = 1 << 2;
/// Flag bit: has_garbage.
const FLAG_HAS_GARBAGE: u8 = 1 << 3;
/// Flag bit: has_sketch.
const FLAG_HAS_SKETCH: u8 = 1 << 4;

/// Inline leaf routing for a single sort order within the default graph.
#[derive(Debug, Clone)]
pub struct InlineOrderRouting {
    pub order: RunSortOrder,
    pub leaves: Vec<LeafEntry>,
}

/// Named graph routing: branch CIDs per sort order.
#[derive(Debug, Clone)]
pub struct NamedGraphRouting {
    pub g_id: GraphId,
    pub orders: Vec<(RunSortOrder, ContentId)>,
}

/// Dictionary refs for v5 (u32 p_id keys for numbig/vectors).
///
/// Forward dictionaries use packed FPK1 format (DictPackRefs).
/// Reverse dictionaries use CoW tree format (DictTreeRefs).
#[derive(Debug, Clone)]
pub struct DictRefsV5 {
    /// Forward dictionary packs (string + subject, FPK1 format).
    pub forward_packs: DictPackRefs,
    /// Subject reverse tree: [ns_code BE][suffix] → sid64 (ns-compressed).
    pub subject_reverse: DictTreeRefs,
    /// String reverse tree: value → string_id.
    pub string_reverse: DictTreeRefs,
    /// Per-predicate numbig arenas, sorted by p_id.
    pub numbig: Vec<(u32, ContentId)>,
    /// Per-predicate vector arenas, sorted by p_id.
    pub vectors: Vec<VectorDictRefV5>,
}

/// Vector arena ref with u32 p_id key.
#[derive(Debug, Clone)]
pub struct VectorDictRefV5 {
    pub p_id: u32,
    pub manifest: ContentId,
    pub shards: Vec<ContentId>,
}

/// Binary index root v5 (`IRB1`).
///
/// All artifact references use `ContentId`. Default graph routing is inline
/// (no branch fetch needed). Named graphs use branch CID pointers.
/// Stats and schema are embedded as binary sections (no JSON).
#[derive(Debug, Clone)]
pub struct IndexRootV5 {
    pub ledger_id: String,
    pub index_t: i64,
    pub base_t: i64,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub namespace_codes: BTreeMap<u16, String>,
    pub predicate_sids: Vec<(u16, String)>,

    // Small dict inlines
    pub graph_iris: Vec<String>,
    pub datatype_iris: Vec<String>,
    pub language_tags: Vec<String>,

    // Dict refs (CID trees)
    pub dict_refs: DictRefsV5,

    // Watermarks
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,

    // Cumulative commit stats
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub total_retracts: u64,

    // Default graph routing (inline)
    pub default_graph_orders: Vec<InlineOrderRouting>,

    // Named graph routing (branch CIDs)
    pub named_graphs: Vec<NamedGraphRouting>,

    // Optional sections
    pub stats: Option<IndexStats>,
    pub schema: Option<IndexSchema>,
    pub prev_index: Option<BinaryPrevIndexRef>,
    pub garbage: Option<BinaryGarbageRef>,
    pub sketch_ref: Option<ContentId>,
}

impl IndexRootV5 {
    /// Encode to the binary IRB1 wire format.
    ///
    /// Determinism: namespaces sorted by ns_code, named graphs by g_id,
    /// orders by order_id, numbig/vectors by p_id.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4096);

        // ---- Header (24 bytes) ----
        buf.extend_from_slice(&ROOT_V5_MAGIC);
        buf.push(ROOT_V5_VERSION);
        let flags = (if self.stats.is_some() {
            FLAG_HAS_STATS
        } else {
            0
        }) | (if self.schema.is_some() {
            FLAG_HAS_SCHEMA
        } else {
            0
        }) | (if self.prev_index.is_some() {
            FLAG_HAS_PREV_INDEX
        } else {
            0
        }) | (if self.garbage.is_some() {
            FLAG_HAS_GARBAGE
        } else {
            0
        }) | (if self.sketch_ref.is_some() {
            FLAG_HAS_SKETCH
        } else {
            0
        });
        buf.push(flags);
        buf.extend_from_slice(&0u16.to_le_bytes()); // pad
        buf.extend_from_slice(&self.index_t.to_le_bytes());
        buf.extend_from_slice(&self.base_t.to_le_bytes());

        // ---- Ledger ID ----
        write_str(&mut buf, &self.ledger_id);

        // ---- Subject ID encoding ----
        buf.push(match self.subject_id_encoding {
            fluree_db_core::SubjectIdEncoding::Narrow => 0,
            fluree_db_core::SubjectIdEncoding::Wide => 1,
        });

        // ---- Namespace codes (sorted by ns_code) ----
        buf.extend_from_slice(&(self.namespace_codes.len() as u16).to_le_bytes());
        for (&ns_code, prefix) in &self.namespace_codes {
            buf.extend_from_slice(&ns_code.to_le_bytes());
            write_str(&mut buf, prefix);
        }

        // ---- Predicate SIDs (ordered by p_id = index position) ----
        buf.extend_from_slice(&(self.predicate_sids.len() as u32).to_le_bytes());
        for (ns_code, suffix) in &self.predicate_sids {
            buf.extend_from_slice(&ns_code.to_le_bytes());
            write_str(&mut buf, suffix);
        }

        // ---- Small dict inlines ----
        write_string_array(&mut buf, &self.graph_iris);
        write_string_array(&mut buf, &self.datatype_iris);
        write_string_array(&mut buf, &self.language_tags);

        // ---- Dict refs ----
        // Forward packs (FPK1)
        write_dict_pack_refs(&mut buf, &self.dict_refs.forward_packs);
        // Reverse trees (DLF1/DTB1)
        write_dict_tree_refs(&mut buf, &self.dict_refs.subject_reverse);
        write_dict_tree_refs(&mut buf, &self.dict_refs.string_reverse);

        // numbig (sorted by p_id)
        let mut sorted_numbig = self.dict_refs.numbig.clone();
        sorted_numbig.sort_by_key(|(p_id, _)| *p_id);
        buf.extend_from_slice(&(sorted_numbig.len() as u16).to_le_bytes());
        for (p_id, cid) in &sorted_numbig {
            buf.extend_from_slice(&p_id.to_le_bytes());
            write_cid(&mut buf, cid);
        }

        // vectors (sorted by p_id)
        let mut sorted_vectors = self.dict_refs.vectors.clone();
        sorted_vectors.sort_by_key(|v| v.p_id);
        buf.extend_from_slice(&(sorted_vectors.len() as u16).to_le_bytes());
        for vdr in &sorted_vectors {
            buf.extend_from_slice(&vdr.p_id.to_le_bytes());
            write_cid(&mut buf, &vdr.manifest);
            buf.extend_from_slice(&(vdr.shards.len() as u16).to_le_bytes());
            for shard_cid in &vdr.shards {
                write_cid(&mut buf, shard_cid);
            }
        }

        // ---- Watermarks ----
        buf.extend_from_slice(&(self.subject_watermarks.len() as u16).to_le_bytes());
        for &wm in &self.subject_watermarks {
            buf.extend_from_slice(&wm.to_le_bytes());
        }
        buf.extend_from_slice(&self.string_watermark.to_le_bytes());

        // ---- Cumulative commit stats ----
        buf.extend_from_slice(&self.total_commit_size.to_le_bytes());
        buf.extend_from_slice(&self.total_asserts.to_le_bytes());
        buf.extend_from_slice(&self.total_retracts.to_le_bytes());

        // ---- Default graph routing (inline) ----
        let mut sorted_default = self.default_graph_orders.clone();
        sorted_default.sort_by_key(|o| o.order.to_wire_id());
        buf.push(sorted_default.len() as u8);
        let mut rec_buf = [0u8; RECORD_WIRE_SIZE];
        for ior in &sorted_default {
            buf.push(ior.order.to_wire_id());
            buf.extend_from_slice(&(ior.leaves.len() as u32).to_le_bytes());
            for leaf in &ior.leaves {
                leaf.first_key.write_le(&mut rec_buf);
                buf.extend_from_slice(&rec_buf);
                leaf.last_key.write_le(&mut rec_buf);
                buf.extend_from_slice(&rec_buf);
                buf.extend_from_slice(&leaf.row_count.to_le_bytes());
                write_cid(&mut buf, &leaf.leaf_cid);
            }
        }

        // ---- Named graph routing (branch CIDs) ----
        let mut sorted_named = self.named_graphs.clone();
        sorted_named.sort_by_key(|ng| ng.g_id);
        buf.extend_from_slice(&(sorted_named.len() as u16).to_le_bytes());
        for ng in &sorted_named {
            buf.extend_from_slice(&ng.g_id.to_le_bytes());
            let mut sorted_orders = ng.orders.clone();
            sorted_orders.sort_by_key(|(order, _)| order.to_wire_id());
            buf.push(sorted_orders.len() as u8);
            for (order, branch_cid) in &sorted_orders {
                buf.push(order.to_wire_id());
                write_cid(&mut buf, branch_cid);
            }
        }

        // ---- Optional: stats ----
        if let Some(ref stats) = self.stats {
            let stats_bytes = stats_wire::encode_stats(stats);
            buf.extend_from_slice(&(stats_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&stats_bytes);
        }

        // ---- Optional: schema ----
        if let Some(ref schema) = self.schema {
            let schema_bytes = stats_wire::encode_schema(schema);
            buf.extend_from_slice(&(schema_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&schema_bytes);
        }

        // ---- Optional: prev_index ----
        if let Some(ref prev) = self.prev_index {
            buf.extend_from_slice(&prev.t.to_le_bytes());
            write_cid(&mut buf, &prev.id);
        }

        // ---- Optional: garbage ----
        if let Some(ref garbage) = self.garbage {
            write_cid(&mut buf, &garbage.id);
        }

        // ---- Optional: sketch_ref ----
        if let Some(ref sketch) = self.sketch_ref {
            write_cid(&mut buf, sketch);
        }

        buf
    }

    /// Decode from IRB1 binary bytes.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < ROOT_V5_HEADER_LEN {
            return Err(io_err("root too small for header"));
        }
        if data[0..4] != ROOT_V5_MAGIC {
            return Err(io_err(&format!(
                "root: expected magic IRB1, got {:?}",
                &data[0..4]
            )));
        }
        let version = data[4];
        if version != ROOT_V5_VERSION {
            return Err(io_err(&format!("root: unsupported version {version}")));
        }

        let flags = data[5];
        let mut pos = 8; // skip pad(2)

        let index_t = read_i64_at(data, &mut pos)?;
        let base_t = read_i64_at(data, &mut pos)?;

        // Ledger ID
        let ledger_id = read_string(data, &mut pos)?;

        // Subject ID encoding
        let enc_byte = read_u8_at(data, &mut pos)?;
        let subject_id_encoding = match enc_byte {
            0 => fluree_db_core::SubjectIdEncoding::Narrow,
            1 => fluree_db_core::SubjectIdEncoding::Wide,
            other => return Err(io_err(&format!("root: unknown encoding {other}"))),
        };

        // Namespace codes
        let ns_count = read_u16_at(data, &mut pos)? as usize;
        let mut namespace_codes = BTreeMap::new();
        for _ in 0..ns_count {
            let ns_code = read_u16_at(data, &mut pos)?;
            let prefix = read_string(data, &mut pos)?;
            namespace_codes.insert(ns_code, prefix);
        }

        // Predicate SIDs
        let pred_count = read_u32_at(data, &mut pos)? as usize;
        let mut predicate_sids = Vec::with_capacity(pred_count);
        for _ in 0..pred_count {
            let ns_code = read_u16_at(data, &mut pos)?;
            let suffix = read_string(data, &mut pos)?;
            predicate_sids.push((ns_code, suffix));
        }

        // Small dict inlines
        let graph_iris = read_string_array(data, &mut pos)?;
        let datatype_iris = read_string_array(data, &mut pos)?;
        let language_tags = read_string_array(data, &mut pos)?;

        // Dict refs
        let forward_packs = read_dict_pack_refs(data, &mut pos)?;
        let subject_reverse = read_dict_tree_refs(data, &mut pos)?;
        let string_reverse = read_dict_tree_refs(data, &mut pos)?;

        // numbig
        let numbig_count = read_u16_at(data, &mut pos)? as usize;
        let mut numbig = Vec::with_capacity(numbig_count);
        for _ in 0..numbig_count {
            let p_id = read_u32_at(data, &mut pos)?;
            let cid = read_cid(data, &mut pos)?;
            numbig.push((p_id, cid));
        }

        // vectors
        let vector_count = read_u16_at(data, &mut pos)? as usize;
        let mut vectors = Vec::with_capacity(vector_count);
        for _ in 0..vector_count {
            let p_id = read_u32_at(data, &mut pos)?;
            let manifest = read_cid(data, &mut pos)?;
            let shard_count = read_u16_at(data, &mut pos)? as usize;
            let mut shards = Vec::with_capacity(shard_count);
            for _ in 0..shard_count {
                shards.push(read_cid(data, &mut pos)?);
            }
            vectors.push(VectorDictRefV5 {
                p_id,
                manifest,
                shards,
            });
        }

        let dict_refs = DictRefsV5 {
            forward_packs,
            subject_reverse,
            string_reverse,
            numbig,
            vectors,
        };

        // Watermarks
        let wm_count = read_u16_at(data, &mut pos)? as usize;
        let mut subject_watermarks = Vec::with_capacity(wm_count);
        for _ in 0..wm_count {
            subject_watermarks.push(read_u64_at(data, &mut pos)?);
        }
        let string_watermark = read_u32_at(data, &mut pos)?;

        // Cumulative commit stats
        let total_commit_size = read_u64_at(data, &mut pos)?;
        let total_asserts = read_u64_at(data, &mut pos)?;
        let total_retracts = read_u64_at(data, &mut pos)?;

        // Default graph routing
        let order_count = read_u8_at(data, &mut pos)? as usize;
        let mut default_graph_orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            let order_id = read_u8_at(data, &mut pos)?;
            let order = RunSortOrder::from_wire_id(order_id)
                .ok_or_else(|| io_err(&format!("root: invalid order_id {order_id}")))?;
            let leaf_count = read_u32_at(data, &mut pos)? as usize;
            let mut leaves = Vec::with_capacity(leaf_count);
            for _ in 0..leaf_count {
                let first_key = read_run_record(data, &mut pos)?;
                let last_key = read_run_record(data, &mut pos)?;
                let row_count = read_u64_at(data, &mut pos)?;
                let leaf_cid = read_cid(data, &mut pos)?;
                leaves.push(LeafEntry {
                    first_key,
                    last_key,
                    row_count,
                    leaf_cid,
                    resolved_path: None,
                });
            }
            default_graph_orders.push(InlineOrderRouting { order, leaves });
        }

        // Named graph routing
        let named_count = read_u16_at(data, &mut pos)? as usize;
        let mut named_graphs = Vec::with_capacity(named_count);
        for _ in 0..named_count {
            let g_id = read_u16_at(data, &mut pos)?;
            let ng_order_count = read_u8_at(data, &mut pos)? as usize;
            let mut orders = Vec::with_capacity(ng_order_count);
            for _ in 0..ng_order_count {
                let oid = read_u8_at(data, &mut pos)?;
                let order = RunSortOrder::from_wire_id(oid)
                    .ok_or_else(|| io_err(&format!("root: invalid named graph order_id {oid}")))?;
                let branch_cid = read_cid(data, &mut pos)?;
                orders.push((order, branch_cid));
            }
            named_graphs.push(NamedGraphRouting { g_id, orders });
        }

        // Optional sections
        let stats = if flags & FLAG_HAS_STATS != 0 {
            let stats_len = read_u32_at(data, &mut pos)? as usize;
            ensure_bytes(data, pos, stats_len, "stats section")?;
            let (stats, _consumed) =
                stats_wire::decode_stats_with_len(&data[pos..pos + stats_len])?;
            pos += stats_len;
            Some(stats)
        } else {
            None
        };

        let schema = if flags & FLAG_HAS_SCHEMA != 0 {
            let schema_len = read_u32_at(data, &mut pos)? as usize;
            ensure_bytes(data, pos, schema_len, "schema section")?;
            let (schema, _consumed) =
                stats_wire::decode_schema_with_len(&data[pos..pos + schema_len])?;
            pos += schema_len;
            Some(schema)
        } else {
            None
        };

        let prev_index = if flags & FLAG_HAS_PREV_INDEX != 0 {
            let t = read_i64_at(data, &mut pos)?;
            let id = read_cid(data, &mut pos)?;
            Some(BinaryPrevIndexRef { t, id })
        } else {
            None
        };

        let garbage = if flags & FLAG_HAS_GARBAGE != 0 {
            let id = read_cid(data, &mut pos)?;
            Some(BinaryGarbageRef { id })
        } else {
            None
        };

        let sketch_ref = if flags & FLAG_HAS_SKETCH != 0 {
            Some(read_cid(data, &mut pos)?)
        } else {
            None
        };

        Ok(IndexRootV5 {
            ledger_id,
            index_t,
            base_t,
            subject_id_encoding,
            namespace_codes,
            predicate_sids,
            graph_iris,
            datatype_iris,
            language_tags,
            dict_refs,
            subject_watermarks,
            string_watermark,
            total_commit_size,
            total_asserts,
            total_retracts,
            default_graph_orders,
            named_graphs,
            stats,
            schema,
            prev_index,
            garbage,
            sketch_ref,
        })
    }

    /// Collect all CAS content-artifact CIDs referenced by this root.
    ///
    /// Includes: dict artifacts, leaf CIDs (default graph inline), branch CIDs
    /// (named graphs), numbig, vectors, sketch. Does NOT include the root's own
    /// CID or the garbage manifest CID.
    pub fn all_cas_ids(&self) -> Vec<ContentId> {
        let mut ids = Vec::new();

        // Forward dict pack CIDs
        for entry in &self.dict_refs.forward_packs.string_fwd_packs {
            ids.push(entry.pack_cid.clone());
        }
        for (_, ns_packs) in &self.dict_refs.forward_packs.subject_fwd_ns_packs {
            for entry in ns_packs {
                ids.push(entry.pack_cid.clone());
            }
        }

        // Reverse dict tree CIDs
        for tree in [
            &self.dict_refs.subject_reverse,
            &self.dict_refs.string_reverse,
        ] {
            ids.push(tree.branch.clone());
            ids.extend(tree.leaves.iter().cloned());
        }

        // Numbig
        for (_, cid) in &self.dict_refs.numbig {
            ids.push(cid.clone());
        }

        // Vectors
        for vdr in &self.dict_refs.vectors {
            ids.push(vdr.manifest.clone());
            ids.extend(vdr.shards.iter().cloned());
        }

        // Default graph inline leaves
        for ior in &self.default_graph_orders {
            for leaf in &ior.leaves {
                ids.push(leaf.leaf_cid.clone());
            }
        }

        // Named graph branch CIDs
        for ng in &self.named_graphs {
            for (_, branch_cid) in &ng.orders {
                ids.push(branch_cid.clone());
            }
        }

        // Sketch
        if let Some(ref sketch) = self.sketch_ref {
            ids.push(sketch.clone());
        }

        ids.sort();
        ids.dedup();
        ids
    }

    /// Write to disk named by CID, returns the root ContentId.
    pub fn write_to_disk(&self, dir: &Path) -> io::Result<ContentId> {
        let data = self.encode();
        let digest_hex = hex::encode(Sha256::digest(&data));
        let cid = ContentId::from_hex_digest(CODEC_FLUREE_INDEX_ROOT, &digest_hex)
            .expect("valid SHA-256 hex digest");
        let path = dir.join(cid.to_string());
        std::fs::write(&path, &data)?;
        Ok(cid)
    }

    /// Human-readable diagnostic JSON (never stored or uploaded).
    pub fn to_debug_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert("format".into(), "IRB1".into());
        obj.insert("version".into(), ROOT_V5_VERSION.into());
        obj.insert("ledger_id".into(), self.ledger_id.clone().into());
        obj.insert("index_t".into(), self.index_t.into());
        obj.insert("base_t".into(), self.base_t.into());
        obj.insert(
            "subject_id_encoding".into(),
            match self.subject_id_encoding {
                fluree_db_core::SubjectIdEncoding::Narrow => "narrow",
                fluree_db_core::SubjectIdEncoding::Wide => "wide",
            }
            .into(),
        );
        obj.insert(
            "namespace_count".into(),
            (self.namespace_codes.len() as u64).into(),
        );
        obj.insert(
            "predicate_count".into(),
            (self.predicate_sids.len() as u64).into(),
        );
        obj.insert(
            "default_graph_orders".into(),
            self.default_graph_orders
                .iter()
                .map(|o| {
                    serde_json::json!({
                        "order": o.order.dir_name(),
                        "leaf_count": o.leaves.len(),
                    })
                })
                .collect::<Vec<_>>()
                .into(),
        );
        obj.insert(
            "named_graphs".into(),
            self.named_graphs
                .iter()
                .map(|ng| {
                    serde_json::json!({
                        "g_id": ng.g_id,
                        "orders": ng.orders.iter().map(|(o, cid)| {
                            serde_json::json!({ "order": o.dir_name(), "branch_cid": cid.to_string() })
                        }).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>()
                .into(),
        );
        obj.insert("has_stats".into(), self.stats.is_some().into());
        obj.insert("has_schema".into(), self.schema.is_some().into());
        obj.insert("has_prev_index".into(), self.prev_index.is_some().into());
        obj.insert("has_garbage".into(), self.garbage.is_some().into());
        obj.insert("has_sketch".into(), self.sketch_ref.is_some().into());
        serde_json::Value::Object(obj)
    }
}

// ============================================================================
// Wire format helpers
// ============================================================================

fn io_err(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn ensure_bytes(data: &[u8], pos: usize, need: usize, ctx: &str) -> io::Result<()> {
    if pos + need > data.len() {
        Err(io_err(&format!(
            "root: truncated at {ctx} (need {need} at offset {pos}, have {})",
            data.len()
        )))
    } else {
        Ok(())
    }
}

fn read_u8_at(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    ensure_bytes(data, *pos, 1, "u8")?;
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16_at(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    ensure_bytes(data, *pos, 2, "u16")?;
    let v = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_u32_at(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    ensure_bytes(data, *pos, 4, "u32")?;
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

fn read_u64_at(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    ensure_bytes(data, *pos, 8, "u64")?;
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

fn read_i64_at(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    ensure_bytes(data, *pos, 8, "i64")?;
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Write a CID as `cid_len:u16(LE) + cid_bytes`.
fn write_cid(buf: &mut Vec<u8>, cid: &ContentId) {
    let cid_bytes = cid.to_bytes();
    buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&cid_bytes);
}

/// Read a CID from `cid_len:u16(LE) + cid_bytes`.
fn read_cid(data: &[u8], pos: &mut usize) -> io::Result<ContentId> {
    let cid_len = read_u16_at(data, pos)? as usize;
    ensure_bytes(data, *pos, cid_len, "cid bytes")?;
    let cid = ContentId::from_bytes(&data[*pos..*pos + cid_len])
        .map_err(|e| io_err(&format!("invalid CID: {e}")))?;
    *pos += cid_len;
    Ok(cid)
}

/// Write a length-prefixed UTF-8 string as `len:u16(LE) + bytes`.
fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Read a length-prefixed UTF-8 string.
fn read_string(data: &[u8], pos: &mut usize) -> io::Result<String> {
    let len = read_u16_at(data, pos)? as usize;
    ensure_bytes(data, *pos, len, "string bytes")?;
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| io_err(&format!("invalid UTF-8: {e}")))?
        .to_string();
    *pos += len;
    Ok(s)
}

/// Write a string array as `count:u16(LE) + [len:u16 + bytes]...`.
fn write_string_array(buf: &mut Vec<u8>, strings: &[String]) {
    buf.extend_from_slice(&(strings.len() as u16).to_le_bytes());
    for s in strings {
        write_str(buf, s);
    }
}

/// Read a string array.
fn read_string_array(data: &[u8], pos: &mut usize) -> io::Result<Vec<String>> {
    let count = read_u16_at(data, pos)? as usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(read_string(data, pos)?);
    }
    Ok(result)
}

/// Write forward dictionary pack refs (FPK1 packs).
///
/// Wire format:
/// ```text
/// [string_fwd_pack_count: u16 LE]
///   For each: [first_id: u64] [last_id: u64] [pack_cid: len_prefixed]
/// [subject_fwd_ns_count: u16 LE]
///   For each ns: [ns_code: u16] [pack_count: u16]
///     For each: [first_id: u64] [last_id: u64] [pack_cid: len_prefixed]
/// ```
fn write_dict_pack_refs(buf: &mut Vec<u8>, packs: &DictPackRefs) {
    // String forward packs
    buf.extend_from_slice(&(packs.string_fwd_packs.len() as u16).to_le_bytes());
    for entry in &packs.string_fwd_packs {
        buf.extend_from_slice(&entry.first_id.to_le_bytes());
        buf.extend_from_slice(&entry.last_id.to_le_bytes());
        write_cid(buf, &entry.pack_cid);
    }

    // Subject forward packs (per namespace)
    let mut sorted_ns = packs.subject_fwd_ns_packs.clone();
    sorted_ns.sort_by_key(|(ns_code, _)| *ns_code);
    buf.extend_from_slice(&(sorted_ns.len() as u16).to_le_bytes());
    for (ns_code, ns_packs) in &sorted_ns {
        buf.extend_from_slice(&ns_code.to_le_bytes());
        buf.extend_from_slice(&(ns_packs.len() as u16).to_le_bytes());
        for entry in ns_packs {
            buf.extend_from_slice(&entry.first_id.to_le_bytes());
            buf.extend_from_slice(&entry.last_id.to_le_bytes());
            write_cid(buf, &entry.pack_cid);
        }
    }
}

/// Read forward dictionary pack refs.
fn read_dict_pack_refs(data: &[u8], pos: &mut usize) -> io::Result<DictPackRefs> {
    // String forward packs
    let str_count = read_u16_at(data, pos)? as usize;
    let mut string_fwd_packs = Vec::with_capacity(str_count);
    for _ in 0..str_count {
        let first_id = read_u64_at(data, pos)?;
        let last_id = read_u64_at(data, pos)?;
        let pack_cid = read_cid(data, pos)?;
        string_fwd_packs.push(PackBranchEntry {
            first_id,
            last_id,
            pack_cid,
        });
    }

    // Subject forward packs (per namespace)
    let ns_count = read_u16_at(data, pos)? as usize;
    let mut subject_fwd_ns_packs = Vec::with_capacity(ns_count);
    for _ in 0..ns_count {
        let ns_code = read_u16_at(data, pos)?;
        let pack_count = read_u16_at(data, pos)? as usize;
        let mut ns_packs = Vec::with_capacity(pack_count);
        for _ in 0..pack_count {
            let first_id = read_u64_at(data, pos)?;
            let last_id = read_u64_at(data, pos)?;
            let pack_cid = read_cid(data, pos)?;
            ns_packs.push(PackBranchEntry {
                first_id,
                last_id,
                pack_cid,
            });
        }
        subject_fwd_ns_packs.push((ns_code, ns_packs));
    }

    Ok(DictPackRefs {
        string_fwd_packs,
        subject_fwd_ns_packs,
    })
}

/// Write dict tree refs: branch CID + leaf_count:u32 + leaf CIDs.
fn write_dict_tree_refs(buf: &mut Vec<u8>, tree: &DictTreeRefs) {
    write_cid(buf, &tree.branch);
    buf.extend_from_slice(&(tree.leaves.len() as u32).to_le_bytes());
    for leaf_cid in &tree.leaves {
        write_cid(buf, leaf_cid);
    }
}

/// Read dict tree refs: branch CID + leaf_count:u32 + leaf CIDs.
fn read_dict_tree_refs(data: &[u8], pos: &mut usize) -> io::Result<DictTreeRefs> {
    let branch = read_cid(data, pos)?;
    let leaf_count = read_u32_at(data, pos)? as usize;
    let mut leaves = Vec::with_capacity(leaf_count);
    for _ in 0..leaf_count {
        leaves.push(read_cid(data, pos)?);
    }
    Ok(DictTreeRefs { branch, leaves })
}

/// Read a RunRecord from wire bytes at position.
fn read_run_record(data: &[u8], pos: &mut usize) -> io::Result<super::run_record::RunRecord> {
    ensure_bytes(data, *pos, RECORD_WIRE_SIZE, "RunRecord")?;
    let rec = super::run_record::RunRecord::read_le(
        data[*pos..*pos + RECORD_WIRE_SIZE].try_into().unwrap(),
    );
    *pos += RECORD_WIRE_SIZE;
    Ok(rec)
}

// ============================================================================
// Tests (v5)
// ============================================================================

#[cfg(test)]
mod tests_v5 {
    use super::*;
    use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::{ContentKind, DatatypeDictId};

    const DICT: ContentKind = ContentKind::DictBlob {
        dict: fluree_db_core::DictKind::Graphs,
    };

    fn test_cid(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn make_record(s_id: u64, p_id: u32, val: i64, t: u32) -> super::super::run_record::RunRecord {
        super::super::run_record::RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    fn make_leaf_cid(index: u32) -> ContentId {
        ContentId::from_hex_digest(
            CODEC_FLUREE_INDEX_LEAF,
            &hex::encode(sha2::Sha256::digest(format!("leaf-{index}").as_bytes())),
        )
        .unwrap()
    }

    fn sample_dict_refs_v5() -> DictRefsV5 {
        DictRefsV5 {
            forward_packs: DictPackRefs {
                string_fwd_packs: vec![PackBranchEntry {
                    first_id: 0,
                    last_id: 999,
                    pack_cid: test_cid(DICT, "str_fwd_pack0"),
                }],
                subject_fwd_ns_packs: vec![(
                    0,
                    vec![PackBranchEntry {
                        first_id: 0,
                        last_id: 499,
                        pack_cid: test_cid(DICT, "subj_fwd_ns0_pack0"),
                    }],
                )],
            },
            subject_reverse: DictTreeRefs {
                branch: test_cid(DICT, "sr_branch"),
                leaves: vec![test_cid(DICT, "sr_l0")],
            },
            string_reverse: DictTreeRefs {
                branch: test_cid(DICT, "str_branch"),
                leaves: vec![test_cid(DICT, "str_l0")],
            },
            numbig: vec![],
            vectors: vec![],
        }
    }

    fn minimal_root() -> IndexRootV5 {
        let mut ns = BTreeMap::new();
        ns.insert(0, String::new());
        ns.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".into());

        IndexRootV5 {
            ledger_id: "test:main".to_string(),
            index_t: 42,
            base_t: 1,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: ns,
            predicate_sids: vec![(0, "p0".to_string())],
            graph_iris: vec!["http://example.org/graph0".to_string()],
            datatype_iris: vec!["@id".to_string()],
            language_tags: vec![],
            dict_refs: sample_dict_refs_v5(),
            subject_watermarks: vec![100, 200],
            string_watermark: 50,
            total_commit_size: 1000,
            total_asserts: 500,
            total_retracts: 50,
            default_graph_orders: vec![],
            named_graphs: vec![],
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
        }
    }

    #[test]
    fn round_trip_minimal() {
        let root = minimal_root();
        let bytes = root.encode();
        let parsed = IndexRootV5::decode(&bytes).unwrap();

        assert_eq!(parsed.ledger_id, "test:main");
        assert_eq!(parsed.index_t, 42);
        assert_eq!(parsed.base_t, 1);
        assert_eq!(parsed.namespace_codes.len(), 2);
        assert_eq!(parsed.predicate_sids.len(), 1);
        assert_eq!(parsed.subject_watermarks, vec![100, 200]);
        assert_eq!(parsed.string_watermark, 50);
        assert_eq!(parsed.total_commit_size, 1000);
        assert_eq!(parsed.total_asserts, 500);
        assert_eq!(parsed.total_retracts, 50);
        assert!(parsed.stats.is_none());
        assert!(parsed.schema.is_none());
        assert!(parsed.prev_index.is_none());
        assert!(parsed.garbage.is_none());
        assert!(parsed.sketch_ref.is_none());
    }

    #[test]
    fn round_trip_with_default_graph_routing() {
        let mut root = minimal_root();
        root.default_graph_orders = vec![InlineOrderRouting {
            order: RunSortOrder::Spot,
            leaves: vec![
                LeafEntry {
                    first_key: make_record(1, 1, 0, 1),
                    last_key: make_record(100, 5, 99, 1),
                    row_count: 5000,
                    leaf_cid: make_leaf_cid(0),
                    resolved_path: None,
                },
                LeafEntry {
                    first_key: make_record(101, 1, 0, 1),
                    last_key: make_record(200, 10, 50, 1),
                    row_count: 5000,
                    leaf_cid: make_leaf_cid(1),
                    resolved_path: None,
                },
            ],
        }];

        let bytes = root.encode();
        let parsed = IndexRootV5::decode(&bytes).unwrap();

        assert_eq!(parsed.default_graph_orders.len(), 1);
        assert_eq!(parsed.default_graph_orders[0].order, RunSortOrder::Spot);
        assert_eq!(parsed.default_graph_orders[0].leaves.len(), 2);
        assert_eq!(
            parsed.default_graph_orders[0].leaves[0]
                .first_key
                .s_id
                .as_u64(),
            1
        );
        assert_eq!(parsed.default_graph_orders[0].leaves[0].row_count, 5000);
        assert_eq!(
            parsed.default_graph_orders[0].leaves[0].leaf_cid,
            make_leaf_cid(0)
        );
        assert_eq!(
            parsed.default_graph_orders[0].leaves[1].leaf_cid,
            make_leaf_cid(1)
        );
    }

    #[test]
    fn round_trip_with_named_graphs() {
        let mut root = minimal_root();
        root.named_graphs = vec![NamedGraphRouting {
            g_id: 1,
            orders: vec![
                (
                    RunSortOrder::Spot,
                    test_cid(ContentKind::IndexBranch, "g1_spot"),
                ),
                (
                    RunSortOrder::Psot,
                    test_cid(ContentKind::IndexBranch, "g1_psot"),
                ),
            ],
        }];

        let bytes = root.encode();
        let parsed = IndexRootV5::decode(&bytes).unwrap();

        assert_eq!(parsed.named_graphs.len(), 1);
        assert_eq!(parsed.named_graphs[0].g_id, 1);
        assert_eq!(parsed.named_graphs[0].orders.len(), 2);
        assert_eq!(parsed.named_graphs[0].orders[0].0, RunSortOrder::Spot);
        assert_eq!(parsed.named_graphs[0].orders[1].0, RunSortOrder::Psot);
    }

    #[test]
    fn round_trip_with_gc_fields() {
        let mut root = minimal_root();
        root.prev_index = Some(BinaryPrevIndexRef {
            t: 40,
            id: test_cid(ContentKind::IndexRoot, "prev_root"),
        });
        root.garbage = Some(BinaryGarbageRef {
            id: test_cid(ContentKind::GarbageRecord, "garbage"),
        });
        root.sketch_ref = Some(test_cid(ContentKind::StatsSketch, "sketch"));

        let bytes = root.encode();
        let parsed = IndexRootV5::decode(&bytes).unwrap();

        assert_eq!(parsed.prev_index.as_ref().unwrap().t, 40);
        assert_eq!(
            parsed.prev_index.as_ref().unwrap().id,
            test_cid(ContentKind::IndexRoot, "prev_root")
        );
        assert!(parsed.garbage.is_some());
        assert!(parsed.sketch_ref.is_some());
    }

    #[test]
    fn round_trip_with_stats_and_schema() {
        use fluree_db_core::index_schema::{SchemaPredicateInfo, SchemaPredicates};
        use fluree_db_core::index_stats::GraphStatsEntry;
        use fluree_db_core::sid::Sid;

        let mut root = minimal_root();
        root.stats = Some(IndexStats {
            flakes: 1000,
            size: 5000,
            properties: None,
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 1000,
                size: 5000,
                properties: vec![],
            }]),
        });
        root.schema = Some(IndexSchema {
            t: 42,
            pred: SchemaPredicates {
                keys: vec![],
                vals: vec![SchemaPredicateInfo {
                    id: Sid::new(5, "Person"),
                    subclass_of: vec![],
                    parent_props: vec![],
                    child_props: vec![],
                }],
            },
        });

        let bytes = root.encode();
        let parsed = IndexRootV5::decode(&bytes).unwrap();

        let stats = parsed.stats.unwrap();
        assert_eq!(stats.flakes, 1000);
        assert_eq!(stats.graphs.as_ref().unwrap().len(), 1);

        let schema = parsed.schema.unwrap();
        assert_eq!(schema.t, 42);
        assert_eq!(schema.pred.vals.len(), 1);
    }

    #[test]
    fn deterministic_encoding() {
        let root = minimal_root();
        let bytes1 = root.encode();
        let bytes2 = root.encode();
        assert_eq!(bytes1, bytes2, "same root must produce identical bytes");
    }

    #[test]
    fn header_structure() {
        let root = minimal_root();
        let bytes = root.encode();

        assert_eq!(&bytes[0..4], b"IRB1");
        assert_eq!(bytes[4], 2); // version
        assert_eq!(bytes[5], 0); // flags (no optional sections)
        assert_eq!(bytes[6], 0); // pad
        assert_eq!(bytes[7], 0); // pad
    }

    #[test]
    fn all_cas_ids_comprehensive() {
        let mut root = minimal_root();
        root.dict_refs.numbig = vec![(5, test_cid(DICT, "numbig_5"))];
        root.default_graph_orders = vec![InlineOrderRouting {
            order: RunSortOrder::Spot,
            leaves: vec![LeafEntry {
                first_key: make_record(1, 1, 0, 1),
                last_key: make_record(100, 5, 0, 1),
                row_count: 5000,
                leaf_cid: make_leaf_cid(0),
                resolved_path: None,
            }],
        }];
        root.named_graphs = vec![NamedGraphRouting {
            g_id: 1,
            orders: vec![(
                RunSortOrder::Spot,
                test_cid(ContentKind::IndexBranch, "g1_spot"),
            )],
        }];
        root.sketch_ref = Some(test_cid(ContentKind::StatsSketch, "sketch"));

        let ids = root.all_cas_ids();

        // 1 str_fwd pack + 1 subj_fwd pack + 2 reverse tree (branch+leaf each = 4) + 1 numbig + 1 inline leaf + 1 named branch + 1 sketch = 10
        assert_eq!(ids.len(), 10);
        assert!(ids.contains(&test_cid(DICT, "numbig_5")));
        assert!(ids.contains(&make_leaf_cid(0)));
        assert!(ids.contains(&test_cid(ContentKind::IndexBranch, "g1_spot")));
        assert!(ids.contains(&test_cid(ContentKind::StatsSketch, "sketch")));
        assert!(ids.contains(&test_cid(DICT, "str_fwd_pack0")));
        assert!(ids.contains(&test_cid(DICT, "subj_fwd_ns0_pack0")));

        // Verify sorted
        for w in ids.windows(2) {
            assert!(w[0] <= w[1]);
        }
    }

    #[test]
    fn debug_json_output() {
        let root = minimal_root();
        let json = root.to_debug_json();
        assert_eq!(json["format"], "IRB1");
        assert_eq!(json["ledger_id"], "test:main");
        assert_eq!(json["index_t"], 42);
        assert_eq!(json["has_stats"], false);
    }

    #[test]
    fn reject_wrong_magic() {
        let root = minimal_root();
        let mut bytes = root.encode();
        bytes[0..4].copy_from_slice(b"BAD!");
        let err = IndexRootV5::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("IRB1"));
    }

    #[test]
    fn write_and_read_from_disk() {
        let dir = std::env::temp_dir().join("fluree_test_root_v5_disk");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let root = minimal_root();
        let cid = root.write_to_disk(&dir).unwrap();

        let path = dir.join(cid.to_string());
        assert!(path.exists());

        let data = std::fs::read(&path).unwrap();
        assert!(cid.verify(&data));

        let parsed = IndexRootV5::decode(&data).unwrap();
        assert_eq!(parsed.ledger_id, "test:main");
        assert_eq!(parsed.index_t, 42);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
