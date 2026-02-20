//! Incremental root assembly: build a new `IndexRootV5` from an old root
//! with selective mutations.
//!
//! The builder clones the old root and provides setters for fields changed
//! during incremental indexing. It accumulates replaced CIDs across all
//! phases for the garbage manifest.

use super::branch::LeafEntry;
use super::index_root::{
    BinaryGarbageRef, BinaryPrevIndexRef, DictRefsV5, IndexRootV5, InlineOrderRouting,
    NamedGraphRouting,
};
use super::run_record::RunSortOrder;
use fluree_db_core::ContentId;
use std::collections::BTreeMap;

/// Builder for an incrementally-updated index root.
///
/// Starts from a clone of the previous root. Setters update only the
/// changed fields. `build()` produces the new root + a list of all
/// replaced CIDs for garbage collection.
pub struct IncrementalRootBuilder {
    root: IndexRootV5,
    replaced_cids: Vec<ContentId>,
}

impl IncrementalRootBuilder {
    /// Create a builder from the previous index root.
    pub fn from_old_root(root: IndexRootV5) -> Self {
        Self {
            root,
            replaced_cids: Vec::new(),
        }
    }

    // ---- Index structure ----

    /// Replace the default graph inline leaf entries for a specific order.
    ///
    /// If the order already exists, it is replaced. Otherwise it is added.
    pub fn set_default_graph_order(&mut self, order: RunSortOrder, leaves: Vec<LeafEntry>) {
        if let Some(existing) = self
            .root
            .default_graph_orders
            .iter_mut()
            .find(|o| o.order == order)
        {
            existing.leaves = leaves;
        } else {
            self.root
                .default_graph_orders
                .push(InlineOrderRouting { order, leaves });
        }
    }

    /// Replace a named graph's branch CID for a specific order.
    pub fn set_named_graph_branch(
        &mut self,
        g_id: u16,
        order: RunSortOrder,
        branch_cid: ContentId,
    ) {
        if let Some(ng) = self.root.named_graphs.iter_mut().find(|n| n.g_id == g_id) {
            if let Some(entry) = ng.orders.iter_mut().find(|o| o.0 == order) {
                entry.1 = branch_cid;
            } else {
                ng.orders.push((order, branch_cid));
            }
        } else {
            self.root.named_graphs.push(NamedGraphRouting {
                g_id,
                orders: vec![(order, branch_cid)],
            });
        }
    }

    // ---- Dictionaries ----

    /// Replace dictionary references.
    pub fn set_dict_refs(&mut self, refs: DictRefsV5) {
        self.root.dict_refs = refs;
    }

    /// Update watermarks.
    pub fn set_watermarks(&mut self, subject_wms: Vec<u64>, string_wm: u32) {
        self.root.subject_watermarks = subject_wms;
        self.root.string_watermark = string_wm;
    }

    // ---- Inline dicts ----

    /// Update predicate SIDs if new predicates were added.
    pub fn set_predicate_sids(&mut self, sids: Vec<(u16, String)>) {
        self.root.predicate_sids = sids;
    }

    /// Update namespace codes if new namespaces were added.
    pub fn set_namespace_codes(&mut self, codes: BTreeMap<u16, String>) {
        self.root.namespace_codes = codes;
    }

    /// Update graph IRIs if new graphs were added.
    pub fn set_graph_iris(&mut self, iris: Vec<String>) {
        self.root.graph_iris = iris;
    }

    /// Update datatype IRIs if new datatypes were added.
    pub fn set_datatype_iris(&mut self, iris: Vec<String>) {
        self.root.datatype_iris = iris;
    }

    /// Update language tags if new languages were added.
    pub fn set_language_tags(&mut self, tags: Vec<String>) {
        self.root.language_tags = tags;
    }

    // ---- Metadata ----

    /// Set the index_t to the new value.
    pub fn set_index_t(&mut self, t: i64) {
        self.root.index_t = t;
    }

    /// Add cumulative commit stats from the incremental window.
    pub fn add_commit_stats(&mut self, size: u64, asserts: u64, retracts: u64) {
        self.root.total_commit_size += size;
        self.root.total_asserts += asserts;
        self.root.total_retracts += retracts;
    }

    // ---- GC ----

    /// Record replaced CIDs from any phase.
    pub fn add_replaced_cids(&mut self, cids: impl IntoIterator<Item = ContentId>) {
        self.replaced_cids.extend(cids);
    }

    /// Set the prev_index reference (pointing to the old root).
    pub fn set_prev_index(&mut self, prev: BinaryPrevIndexRef) {
        self.root.prev_index = Some(prev);
    }

    /// Set the garbage reference (pointing to the garbage manifest blob).
    pub fn set_garbage(&mut self, garbage: BinaryGarbageRef) {
        self.root.garbage = Some(garbage);
    }

    // ---- Build ----

    /// Consume the builder and return the new root + all replaced CIDs.
    ///
    /// The caller should:
    /// 1. Encode the root via `root.encode()`
    /// 2. Upload to CAS
    /// 3. Build a garbage manifest from `replaced_cids` and upload
    /// 4. Set the garbage ref if desired
    pub fn build(self) -> (IndexRootV5, Vec<ContentId>) {
        (self.root, self.replaced_cids)
    }

    /// Access the root being built (for inspection).
    pub fn root(&self) -> &IndexRootV5 {
        &self.root
    }

    /// Access the accumulated replaced CIDs.
    pub fn replaced_cids(&self) -> &[ContentId] {
        &self.replaced_cids
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::index_root::DictPackRefs;
    use super::super::index_root::DictTreeRefs;
    use super::*;
    use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT;
    use fluree_db_core::SubjectIdEncoding;

    fn dummy_cid(label: &str) -> ContentId {
        ContentId::from_hex_digest(
            CODEC_FLUREE_INDEX_ROOT,
            &fluree_db_core::sha256_hex(label.as_bytes()),
        )
        .unwrap()
    }

    fn make_test_root() -> IndexRootV5 {
        IndexRootV5 {
            ledger_id: "test-ledger".to_string(),
            index_t: 5,
            base_t: 0,
            subject_id_encoding: SubjectIdEncoding::Wide,
            namespace_codes: BTreeMap::from([
                (0, "".to_string()),
                (1, "http://example.org/".to_string()),
            ]),
            predicate_sids: vec![(1, "name".to_string()), (1, "age".to_string())],
            graph_iris: vec!["urn:fluree:txn-meta".to_string()],
            datatype_iris: vec!["xsd:string".to_string()],
            language_tags: vec!["en".to_string()],
            dict_refs: DictRefsV5 {
                forward_packs: DictPackRefs {
                    string_fwd_packs: vec![],
                    subject_fwd_ns_packs: vec![],
                },
                subject_reverse: DictTreeRefs {
                    branch: dummy_cid("subj-rev-branch"),
                    leaves: vec![dummy_cid("subj-rev-leaf-0")],
                },
                string_reverse: DictTreeRefs {
                    branch: dummy_cid("str-rev-branch"),
                    leaves: vec![dummy_cid("str-rev-leaf-0")],
                },
            },
            subject_watermarks: vec![100, 50],
            string_watermark: 200,
            total_commit_size: 1000,
            total_asserts: 500,
            total_retracts: 10,
            graph_arenas: vec![],
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
    fn test_identity_build() {
        let root = make_test_root();
        let original_t = root.index_t;
        let builder = IncrementalRootBuilder::from_old_root(root);
        let (new_root, replaced) = builder.build();

        assert_eq!(new_root.index_t, original_t);
        assert!(replaced.is_empty());
    }

    #[test]
    fn test_set_index_t() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);
        builder.set_index_t(10);
        let (new_root, _) = builder.build();
        assert_eq!(new_root.index_t, 10);
    }

    #[test]
    fn test_add_commit_stats() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);
        builder.add_commit_stats(500, 200, 5);
        let (new_root, _) = builder.build();
        assert_eq!(new_root.total_commit_size, 1500);
        assert_eq!(new_root.total_asserts, 700);
        assert_eq!(new_root.total_retracts, 15);
    }

    #[test]
    fn test_gc_tracking() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);

        let cid1 = dummy_cid("replaced-1");
        let cid2 = dummy_cid("replaced-2");
        let cid3 = dummy_cid("replaced-3");

        builder.add_replaced_cids([cid1.clone(), cid2.clone()]);
        builder.add_replaced_cids([cid3.clone()]);

        let (_, replaced) = builder.build();
        assert_eq!(replaced.len(), 3);
        assert_eq!(replaced[0], cid1);
        assert_eq!(replaced[1], cid2);
        assert_eq!(replaced[2], cid3);
    }

    #[test]
    fn test_set_watermarks() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);
        builder.set_watermarks(vec![200, 100, 50], 500);
        let (new_root, _) = builder.build();
        assert_eq!(new_root.subject_watermarks, vec![200, 100, 50]);
        assert_eq!(new_root.string_watermark, 500);
    }

    #[test]
    fn test_set_prev_index() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);
        let prev_cid = dummy_cid("old-root");
        builder.set_prev_index(BinaryPrevIndexRef {
            t: 5,
            id: prev_cid.clone(),
        });
        let (new_root, _) = builder.build();
        assert!(new_root.prev_index.is_some());
        assert_eq!(new_root.prev_index.unwrap().id, prev_cid);
    }

    #[test]
    fn test_set_default_graph_order_replace() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);

        // Add SPOT order
        builder.set_default_graph_order(RunSortOrder::Spot, vec![]);
        assert_eq!(builder.root().default_graph_orders.len(), 1);

        // Replace SPOT order
        builder.set_default_graph_order(RunSortOrder::Spot, vec![]);
        assert_eq!(builder.root().default_graph_orders.len(), 1);

        // Add PSOT order
        builder.set_default_graph_order(RunSortOrder::Psot, vec![]);
        assert_eq!(builder.root().default_graph_orders.len(), 2);
    }

    #[test]
    fn test_set_named_graph_branch() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);

        let cid1 = dummy_cid("branch-g1-spot");
        let cid2 = dummy_cid("branch-g1-psot");

        builder.set_named_graph_branch(1, RunSortOrder::Spot, cid1.clone());
        builder.set_named_graph_branch(1, RunSortOrder::Psot, cid2.clone());

        let (new_root, _) = builder.build();
        assert_eq!(new_root.named_graphs.len(), 1);
        assert_eq!(new_root.named_graphs[0].g_id, 1);
        assert_eq!(new_root.named_graphs[0].orders.len(), 2);
    }

    #[test]
    fn test_encode_decode_round_trip() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);
        builder.set_index_t(10);
        builder.add_commit_stats(100, 50, 2);
        builder.set_watermarks(vec![200, 100], 300);

        let (new_root, _) = builder.build();
        let encoded = new_root.encode();
        let decoded = IndexRootV5::decode(&encoded).unwrap();

        assert_eq!(decoded.index_t, 10);
        assert_eq!(decoded.total_commit_size, 1100);
        assert_eq!(decoded.subject_watermarks, vec![200, 100]);
        assert_eq!(decoded.string_watermark, 300);
    }

    /// Simulates the full pipeline flow: replaced CIDs from leaf merges,
    /// branch updates, and dict updates are all accumulated, then the
    /// garbage ref is set on the root and survives encode/decode.
    #[test]
    fn test_gc_multi_phase_accumulation_and_round_trip() {
        let root = make_test_root();
        let mut builder = IncrementalRootBuilder::from_old_root(root);

        // Phase 2-3: leaf merge replaced old leaves
        let leaf_cid_1 = dummy_cid("old-leaf-spot-0");
        let leaf_cid_2 = dummy_cid("old-leaf-spot-1");
        builder.add_replaced_cids([leaf_cid_1.clone(), leaf_cid_2.clone()]);

        // Phase 3: branch update replaced old FBR2 manifest
        let branch_cid = dummy_cid("old-branch-g1-spot");
        builder.add_replaced_cids([branch_cid.clone()]);

        // Phase 4: dict update replaced old reverse tree branch + leaf
        let dict_branch_cid = dummy_cid("old-subj-rev-branch");
        let dict_leaf_cid = dummy_cid("old-subj-rev-leaf");
        builder.add_replaced_cids([dict_branch_cid.clone(), dict_leaf_cid.clone()]);

        // Phase 4: forward pack tail replaced
        let pack_cid = dummy_cid("old-tail-pack");
        builder.add_replaced_cids([pack_cid.clone()]);

        // Verify all 6 CIDs accumulated
        assert_eq!(builder.replaced_cids().len(), 6);

        // Build root + set garbage ref (as the pipeline does post-build)
        let (mut final_root, replaced) = builder.build();
        assert_eq!(replaced.len(), 6);

        // Pipeline writes a garbage record from `replaced` and sets ref on root
        let garbage_manifest_cid = dummy_cid("garbage-manifest");
        final_root.garbage = Some(BinaryGarbageRef {
            id: garbage_manifest_cid.clone(),
        });

        // Also set prev_index (pipeline always does this)
        final_root.prev_index = Some(BinaryPrevIndexRef {
            t: 5,
            id: dummy_cid("old-root"),
        });

        // Round-trip encode/decode
        let encoded = final_root.encode();
        let decoded = IndexRootV5::decode(&encoded).unwrap();

        // Garbage ref survives round-trip
        assert!(decoded.garbage.is_some());
        assert_eq!(decoded.garbage.unwrap().id, garbage_manifest_cid);

        // Prev index survives round-trip
        assert!(decoded.prev_index.is_some());
        assert_eq!(decoded.prev_index.unwrap().t, 5);
    }
}
