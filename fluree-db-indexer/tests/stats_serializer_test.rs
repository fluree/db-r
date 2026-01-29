#[cfg(feature = "hll-stats")]
mod tests {
    use fluree_db_core::serde::json::PropertyStatEntry;
    use fluree_db_core::storage::MemoryStorage;
    use fluree_db_core::StorageWrite;
    use fluree_db_indexer::stats::{
        list_hll_sketch_addresses, load_hll_sketches, persist_hll_sketches, PropertyHll,
    };
    use fluree_db_core::Sid;

    #[tokio::test]
    async fn load_hll_sketches_handles_wrong_size_bytes() {
        let storage = MemoryStorage::new();
        let alias = "test/ledger";

        let sid = Sid::new(200, "name");
        let mut hll = PropertyHll::new();
        hll.count = 10;
        hll.last_modified_t = 42;
        hll.values_hll.insert_hash(0x123456789abcdef0);
        hll.subjects_hll.insert_hash(0x0fedcba987654321);

        let mut properties = std::collections::HashMap::new();
        properties.insert(sid.clone(), hll.clone());

        let _addresses = persist_hll_sketches(&storage, alias, &properties)
            .await
            .expect("persist should succeed");

        // Corrupt the values sketch with wrong-sized payload
        let addresses = list_hll_sketch_addresses(alias, &properties);
        let values_addr = addresses
            .iter()
            .find(|addr| addr.contains("/values/"))
            .expect("values address exists");
        storage
            .write_bytes(values_addr, &[0u8; 10])
            .await
            .expect("overwrite should succeed");

        let entries = vec![PropertyStatEntry {
            sid: (sid.namespace_code, sid.name.as_ref().to_string()),
            count: hll.count,
            ndv_values: 0,
            ndv_subjects: 0,
            last_modified_t: hll.last_modified_t,
        }];

        let loaded = load_hll_sketches(&storage, alias, &entries)
            .await
            .expect("load should succeed");

        let loaded_hll = loaded.get(&sid).expect("property should load");
        assert_eq!(loaded_hll.count, hll.count);
        assert_eq!(loaded_hll.last_modified_t, hll.last_modified_t);
        assert!(loaded_hll.values_hll.is_empty(), "values sketch should be empty after corruption");
        assert!(
            !loaded_hll.subjects_hll.is_empty(),
            "subjects sketch should remain intact"
        );
    }
}
