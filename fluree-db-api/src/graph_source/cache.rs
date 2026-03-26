//! Caching infrastructure for graph source operations.
//!
//! Provides caches for R2RML compiled mappings and Iceberg table metadata
//! to avoid repeated expensive operations.

use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "iceberg")]
use fluree_db_iceberg::{io::parquet::ParquetFooterCache, metadata::TableMetadata, DataFile};
#[cfg(feature = "iceberg")]
use std::time::{Duration, Instant};

#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub(crate) struct CachedScanFiles {
    pub data_files: Arc<Vec<DataFile>>,
    pub estimated_row_count: i64,
    pub files_selected: usize,
    pub files_pruned: usize,
}

#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
struct CachedMetadataLocation {
    metadata_location: String,
    cached_at: Instant,
}

#[cfg(feature = "iceberg")]
const DIRECT_METADATA_LOCATION_TTL: Duration = Duration::from_secs(2);

/// Cache for R2RML compiled mappings and Iceberg table metadata.
///
/// This cache is shared across queries to avoid repeated:
/// - R2RML mapping compilation (parsing + validation)
/// - Iceberg catalog calls (load table metadata)
/// - S3 metadata reads
///
/// # Cache Keys
///
/// - **Compiled mappings**: Keyed by `(graph_source_id, mapping_source)` - invalidated when
///   graph source config changes or mapping file is updated.
/// - **Table metadata**: Keyed by `metadata_location` - the S3 path is a content hash,
///   so different snapshots have different keys.
///
/// # Thread Safety
///
/// Uses `RwLock<LruCache>` for safe concurrent access.
#[derive(Debug)]
pub struct R2rmlCache {
    /// Cache for compiled R2RML mappings.
    /// Key: `(graph_source_id, mapping_source_hash)`
    /// Value: Compiled mapping
    compiled_mappings: RwLock<LruCache<String, Arc<CompiledR2rmlMapping>>>,

    /// Cache for parsed Iceberg table metadata.
    /// Key: `metadata_location` (S3 path, which is content-addressed)
    /// Value: Parsed TableMetadata
    #[cfg(feature = "iceberg")]
    table_metadata: RwLock<LruCache<String, Arc<TableMetadata>>>,

    /// Cache for manifest-derived file selections keyed by metadata location.
    ///
    /// This avoids reparsing manifest lists/manifests for repeated scans of the
    /// same Iceberg snapshot with different projections.
    #[cfg(feature = "iceberg")]
    scan_files: RwLock<LruCache<String, Arc<CachedScanFiles>>>,

    /// Shared Parquet footer cache for repeated scans of the same files.
    #[cfg(feature = "iceberg")]
    parquet_footers: ParquetFooterCache,

    /// Short-lived cache for direct-catalog `version-hint.text` resolution.
    ///
    /// This is intentionally time-bounded so repeated scans within the same
    /// query or warm invocation can reuse the metadata location without pinning
    /// a stale snapshot for long if a writer advances the table.
    #[cfg(feature = "iceberg")]
    direct_metadata_locations: RwLock<LruCache<String, CachedMetadataLocation>>,
}

impl R2rmlCache {
    /// Create a new cache with specified capacities.
    ///
    /// # Arguments
    ///
    /// * `mapping_capacity` - Max compiled mappings to cache (default: 64)
    /// * `metadata_capacity` - Max table metadata entries to cache (default: 128)
    pub fn new(mapping_capacity: usize, metadata_capacity: usize) -> Self {
        #[cfg(feature = "iceberg")]
        {
            Self {
                compiled_mappings: RwLock::new(LruCache::new(
                    NonZeroUsize::new(mapping_capacity.max(1)).unwrap(),
                )),
                table_metadata: RwLock::new(LruCache::new(
                    NonZeroUsize::new(metadata_capacity.max(1)).unwrap(),
                )),
                scan_files: RwLock::new(LruCache::new(
                    NonZeroUsize::new(metadata_capacity.max(1)).unwrap(),
                )),
                parquet_footers: ParquetFooterCache::new((metadata_capacity.max(1) / 2).max(32)),
                direct_metadata_locations: RwLock::new(LruCache::new(
                    NonZeroUsize::new(metadata_capacity.max(1)).unwrap(),
                )),
            }
        }

        #[cfg(not(feature = "iceberg"))]
        {
            let _ = metadata_capacity;
            Self {
                compiled_mappings: RwLock::new(LruCache::new(
                    NonZeroUsize::new(mapping_capacity.max(1)).unwrap(),
                )),
            }
        }
    }

    /// Create a cache with default capacities.
    pub fn with_defaults() -> Self {
        Self::new(64, 128)
    }

    /// Get a compiled mapping from cache.
    pub async fn get_mapping(&self, cache_key: &str) -> Option<Arc<CompiledR2rmlMapping>> {
        let mut cache = self.compiled_mappings.write().await;
        cache.get(cache_key).cloned()
    }

    /// Store a compiled mapping in cache.
    pub async fn put_mapping(&self, cache_key: String, mapping: Arc<CompiledR2rmlMapping>) {
        let mut cache = self.compiled_mappings.write().await;
        cache.put(cache_key, mapping);
    }

    /// Get table metadata from cache.
    #[cfg(feature = "iceberg")]
    pub async fn get_metadata(&self, metadata_location: &str) -> Option<Arc<TableMetadata>> {
        let mut cache = self.table_metadata.write().await;
        cache.get(metadata_location).cloned()
    }

    /// Store table metadata in cache.
    #[cfg(feature = "iceberg")]
    pub async fn put_metadata(&self, metadata_location: String, metadata: Arc<TableMetadata>) {
        let mut cache = self.table_metadata.write().await;
        cache.put(metadata_location, metadata);
    }

    /// Get cached scan file selections for a metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_scan_files(
        &self,
        metadata_location: &str,
    ) -> Option<Arc<CachedScanFiles>> {
        let mut cache = self.scan_files.write().await;
        cache.get(metadata_location).cloned()
    }

    /// Store manifest-derived scan file selections for a metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_scan_files(
        &self,
        metadata_location: String,
        scan_files: Arc<CachedScanFiles>,
    ) {
        let mut cache = self.scan_files.write().await;
        cache.put(metadata_location, scan_files);
    }

    /// Get the shared Parquet footer cache.
    #[cfg(feature = "iceberg")]
    pub fn parquet_footers(&self) -> &ParquetFooterCache {
        &self.parquet_footers
    }

    /// Get a recently resolved direct-catalog metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_direct_metadata_location(
        &self,
        table_location: &str,
    ) -> Option<String> {
        let mut cache = self.direct_metadata_locations.write().await;
        if let Some(cached) = cache.get(table_location) {
            if cached.cached_at.elapsed() <= DIRECT_METADATA_LOCATION_TTL {
                return Some(cached.metadata_location.clone());
            }
        }
        cache.pop(table_location);
        None
    }

    /// Store a resolved direct-catalog metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_direct_metadata_location(
        &self,
        table_location: String,
        metadata_location: String,
    ) {
        let mut cache = self.direct_metadata_locations.write().await;
        cache.put(
            table_location,
            CachedMetadataLocation {
                metadata_location,
                cached_at: Instant::now(),
            },
        );
    }

    /// Clear all caches.
    pub async fn clear(&self) {
        let mut mappings = self.compiled_mappings.write().await;
        mappings.clear();

        #[cfg(feature = "iceberg")]
        {
            let mut metadata = self.table_metadata.write().await;
            metadata.clear();

            let mut scan_files = self.scan_files.write().await;
            scan_files.clear();

            self.parquet_footers.clear().await;

            let mut direct_locations = self.direct_metadata_locations.write().await;
            direct_locations.clear();
        }
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> R2rmlCacheStats {
        let mappings = self.compiled_mappings.read().await;
        R2rmlCacheStats {
            mapping_entries: mappings.len(),
            mapping_capacity: mappings.cap().get(),
            metadata_entries: {
                #[cfg(feature = "iceberg")]
                {
                    let metadata = self.table_metadata.read().await;
                    metadata.len()
                }
                #[cfg(not(feature = "iceberg"))]
                {
                    0
                }
            },
            metadata_capacity: {
                #[cfg(feature = "iceberg")]
                {
                    let metadata = self.table_metadata.read().await;
                    metadata.cap().get()
                }
                #[cfg(not(feature = "iceberg"))]
                {
                    0
                }
            },
        }
    }

    /// Generate a cache key for a compiled mapping.
    ///
    /// Uses `graph_source_id` + hash of `mapping_source` to handle both graph source identity
    /// and mapping file updates.
    ///
    /// The key includes:
    /// - `graph_source_id` - ensures different graph sources don't share mappings
    /// - `mapping_source` - the storage path/address
    /// - `media_type` - distinguishes same source parsed as different formats
    ///
    /// Note: This does NOT detect content changes at the same path.
    /// Use `r2rml_cache().clear()` to invalidate after updating mapping files.
    pub fn mapping_cache_key(
        graph_source_id: &str,
        mapping_source: &str,
        media_type: Option<&str>,
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        mapping_source.hash(&mut hasher);
        media_type.hash(&mut hasher);
        let combined_hash = hasher.finish();

        format!("{}:{:016x}", graph_source_id, combined_hash)
    }
}

impl Default for R2rmlCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Statistics for R2RML cache usage.
#[derive(Debug, Clone)]
pub struct R2rmlCacheStats {
    /// Number of cached compiled mappings
    pub mapping_entries: usize,
    /// Maximum mapping cache capacity
    pub mapping_capacity: usize,
    /// Number of cached table metadata entries
    pub metadata_entries: usize,
    /// Maximum metadata cache capacity
    pub metadata_capacity: usize,
}
