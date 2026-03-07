//! IPFS connection support via Kubo HTTP RPC.

use crate::config::{ConnectionConfig, IpfsStorageConfig};
use crate::connection::Connection;
use fluree_db_storage_ipfs::{IpfsConfig, IpfsStorage};

/// Type alias for an IPFS-backed connection.
pub type IpfsConnection = Connection<IpfsStorage>;

/// Create an IPFS-backed connection from parsed config.
pub fn connect_ipfs(config: ConnectionConfig, ipfs_config: &IpfsStorageConfig) -> IpfsConnection {
    let storage = IpfsStorage::new(IpfsConfig {
        api_url: ipfs_config.api_url.clone(),
        pin_on_put: ipfs_config.pin_on_put,
    });
    Connection::new(config, storage)
}
