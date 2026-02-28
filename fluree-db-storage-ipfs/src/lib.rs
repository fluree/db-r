//! IPFS storage backend for Fluree DB.
//!
//! Provides both `ContentStore` and `Storage` implementations backed by IPFS
//! via the Kubo HTTP RPC API (`/api/v0/block/*`).
//!
//! ## Architecture
//!
//! Fluree's `ContentId` is a CIDv1 with SHA2-256 multihash and Fluree-specific
//! multicodec values (private-use range 0x300001–0x30000B). Kubo accepts these
//! custom codecs in `block/put` and resolves blocks by multihash — so Fluree's
//! native CIDs work directly with IPFS, no translation layer needed.
//!
//! The `Storage` trait implementation bridges the address-based API
//! (`fluree:ipfs://path/{hash}.ext`) to CID-based IPFS operations by extracting
//! the SHA-256 hash from addresses.
//!
//! ## Usage
//!
//! Requires a running Kubo node with the HTTP RPC API enabled (default port 5001).

pub mod address;
pub mod error;
pub mod kubo;

pub use error::{IpfsStorageError, Result};
pub use kubo::KuboClient;

use async_trait::async_trait;
use fluree_db_core::content_id::ContentId;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::storage::{
    ContentAddressedWrite, ContentStore, ContentWriteResult, StorageMethod, StorageRead,
    StorageWrite,
};

/// IPFS storage backend backed by a Kubo node.
///
/// Implements both `ContentStore` (CID-first) and the full `Storage` trait
/// (address-based), enabling use with `Fluree<IpfsStorage, N>` generics.
#[derive(Debug, Clone)]
pub struct IpfsStorage {
    kubo: KuboClient,
    /// If true, pin every block on put. Defaults to true.
    pin_on_put: bool,
}

/// Configuration for `IpfsStorage`.
#[derive(Debug, Clone)]
pub struct IpfsConfig {
    /// Kubo RPC API base URL (e.g., `http://127.0.0.1:5001`).
    pub api_url: String,
    /// Pin blocks on put. Default: true.
    pub pin_on_put: bool,
}

impl Default for IpfsConfig {
    fn default() -> Self {
        Self {
            api_url: "http://127.0.0.1:5001".to_string(),
            pin_on_put: true,
        }
    }
}

impl IpfsStorage {
    /// Create a new IPFS storage backend.
    pub fn new(config: IpfsConfig) -> Self {
        Self {
            kubo: KuboClient::new(config.api_url),
            pin_on_put: config.pin_on_put,
        }
    }

    /// Create from an existing `KuboClient`.
    pub fn from_client(kubo: KuboClient, pin_on_put: bool) -> Self {
        Self { kubo, pin_on_put }
    }

    /// Check if the backing Kubo node is reachable.
    pub async fn is_available(&self) -> bool {
        self.kubo.is_available().await
    }

    /// Access the underlying Kubo client.
    pub fn kubo(&self) -> &KuboClient {
        &self.kubo
    }

    /// Format the Fluree multicodec as a hex string for Kubo's `cid-codec` parameter.
    fn codec_hex(kind: ContentKind) -> String {
        format!("0x{:x}", kind.to_codec())
    }

    /// Pin a block if pin_on_put is enabled.
    ///
    /// Converts the CID to raw codec (0x55) before pinning. Kubo rejects
    /// `pin/add` for CIDs with unregistered codecs (like Fluree's private-use
    /// range) because it tries to decode the block for DAG traversal. Using
    /// raw codec avoids this — Kubo treats the block as opaque bytes and pins
    /// it by multihash, which is all we need.
    async fn maybe_pin(&self, cid: &str) {
        if self.pin_on_put {
            let pin_cid = match Self::to_raw_cid(cid) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(cid = %cid, error = %e, "failed to convert CID for pinning");
                    return;
                }
            };
            if let Err(e) = self.kubo.pin_add(&pin_cid).await {
                tracing::warn!(cid = %cid, raw_cid = %pin_cid, error = %e, "failed to pin block");
            }
        }
    }

    /// Convert any CID string to its raw-codec (0x55) equivalent with the same
    /// multihash. Used for pin operations since Kubo can't pin custom codecs.
    fn to_raw_cid(cid_str: &str) -> std::result::Result<String, String> {
        let cid = cid::Cid::try_from(cid_str).map_err(|e| format!("invalid CID: {e}"))?;
        let raw = cid::Cid::new_v1(0x55, *cid.hash());
        Ok(raw.to_string())
    }
}

// ============================================================================
// ContentStore (CID-first interface)
// ============================================================================

#[async_trait]
impl ContentStore for IpfsStorage {
    async fn has(&self, id: &ContentId) -> fluree_db_core::error::Result<bool> {
        let cid_str = id.to_string();
        match self.kubo.block_stat(&cid_str).await {
            Ok(_) => Ok(true),
            Err(IpfsStorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(fluree_db_core::error::Error::storage(e.to_string())),
        }
    }

    async fn get(&self, id: &ContentId) -> fluree_db_core::error::Result<Vec<u8>> {
        let cid_str = id.to_string();
        self.kubo
            .block_get(&cid_str)
            .await
            .map_err(fluree_db_core::error::Error::from)
    }

    async fn put(
        &self,
        kind: ContentKind,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentId> {
        let expected_id = ContentId::new(kind, bytes);
        let codec = Self::codec_hex(kind);

        let response = self
            .kubo
            .block_put(bytes, Some(&codec), Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;

        tracing::debug!(
            fluree_cid = %expected_id,
            ipfs_cid = %response.key,
            size = response.size,
            "block put to IPFS"
        );

        self.maybe_pin(&response.key).await;
        Ok(expected_id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::error::Result<()> {
        if !id.verify(bytes) {
            return Err(fluree_db_core::error::Error::storage(format!(
                "CID verification failed: provided CID {} does not match bytes",
                id
            )));
        }

        // Use the codec from the CID itself
        let codec = format!("0x{:x}", id.codec());

        let response = self
            .kubo
            .block_put(bytes, Some(&codec), Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;

        tracing::debug!(
            fluree_cid = %id,
            ipfs_cid = %response.key,
            size = response.size,
            "block put_with_id to IPFS"
        );

        self.maybe_pin(&response.key).await;
        Ok(())
    }
}

// ============================================================================
// Storage traits (address-based interface)
// ============================================================================

impl StorageMethod for IpfsStorage {
    fn storage_method(&self) -> &str {
        fluree_db_core::STORAGE_METHOD_IPFS
    }
}

#[async_trait]
impl StorageRead for IpfsStorage {
    async fn read_bytes(&self, addr: &str) -> fluree_db_core::error::Result<Vec<u8>> {
        let hash_hex = address::extract_hash_hex(addr)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        let cid_str = address::hash_hex_to_cid_string(hash_hex)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        self.kubo
            .block_get(&cid_str)
            .await
            .map_err(fluree_db_core::error::Error::from)
    }

    async fn exists(&self, addr: &str) -> fluree_db_core::error::Result<bool> {
        let hash_hex = address::extract_hash_hex(addr)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        let cid_str = address::hash_hex_to_cid_string(hash_hex)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        match self.kubo.block_stat(&cid_str).await {
            Ok(_) => Ok(true),
            Err(IpfsStorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(fluree_db_core::error::Error::storage(e.to_string())),
        }
    }

    async fn list_prefix(&self, _prefix: &str) -> fluree_db_core::error::Result<Vec<String>> {
        // IPFS is a content-addressed store — there is no concept of prefix
        // listing. Admin operations that require this (drop, GC) must use
        // alternative strategies with IPFS (e.g., manifest-based tracking).
        Err(fluree_db_core::error::Error::storage(
            "IPFS does not support prefix listing; use manifest-based tracking instead",
        ))
    }
}

#[async_trait]
impl StorageWrite for IpfsStorage {
    async fn write_bytes(&self, _addr: &str, bytes: &[u8]) -> fluree_db_core::error::Result<()> {
        // For non-CAS writes (e.g., nameservice records), store as raw blocks.
        let response = self
            .kubo
            .block_put(bytes, None, Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        self.maybe_pin(&response.key).await;
        Ok(())
    }

    async fn delete(&self, addr: &str) -> fluree_db_core::error::Result<()> {
        // IPFS content is immutable — "deletion" means unpinning so the block
        // becomes eligible for Kubo's garbage collector.
        let hash_hex = address::extract_hash_hex(addr)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        let cid_str = address::hash_hex_to_cid_string(hash_hex)
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        self.kubo
            .pin_rm(&cid_str)
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ContentAddressedWrite for IpfsStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        _ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentWriteResult> {
        let codec = Self::codec_hex(kind);

        let response = self
            .kubo
            .block_put(bytes, Some(&codec), Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;

        tracing::debug!(
            ipfs_cid = %response.key,
            hash = %content_hash_hex,
            size = response.size,
            "content write to IPFS"
        );

        self.maybe_pin(&response.key).await;

        // Build the canonical Fluree address for this content
        let address = fluree_db_core::storage::content_address(
            fluree_db_core::STORAGE_METHOD_IPFS,
            kind,
            _ledger_id,
            content_hash_hex,
        );

        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}
