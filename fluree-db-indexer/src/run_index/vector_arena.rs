//! Per-predicate vector arena storing packed f32 arrays.
//!
//! ## Precision contract
//!
//! The `f:vector` / `@vector` datatype is defined as **f32 storage**.
//! Values are quantized to IEEE-754 binary32 at ingest (in the coercion
//! layer) and stored in contiguous packed f32 arrays here. Non-finite
//! values (NaN, ±Inf) and values outside f32 range are rejected at ingest.
//!
//! Users requiring higher-precision or non-float vectors (e.g. f64, integer,
//! sparse) should use a custom RDF datatype, which Fluree stores as a
//! string literal.
//!
//! Each vector gets a sequential `u32` handle stored in `ObjKey` with
//! `ObjKind::VECTOR_ID`.
//!
//! ## On-disk formats
//!
//! **VAS1** (Vector Arena Shard): binary, CAS-addressed, 16-byte-aligned header.
//! ```text
//! Magic: "VAS1"       (4 bytes)
//! version: u8          (= 1)
//! dims: u16 LE
//! count: u32 LE
//! _pad: [u8; 5]        (zero, aligns header to 16 bytes)
//! data: [f32 LE; count * dims]
//! ```
//!
//! **VAM1** (Vector Arena Manifest): JSON, CAS-addressed.
//! ```json
//! {
//!     "version": 1,
//!     "dims": 768,
//!     "dtype": "f32",
//!     "normalized": true,
//!     "shard_capacity": 3072,
//!     "total_count": 50000,
//!     "shards": [
//!         { "cas": "fluree:file://...", "count": 3072 },
//!         { "cas": "fluree:file://...", "count": 784 }
//!     ]
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;

/// Maximum vectors per shard. At 768-dim f32 each shard ≈ 9 MB.
pub const SHARD_CAPACITY: u32 = 3072;

/// Epsilon for unit-norm check: |1.0 - ‖v‖²| < ε.
const UNIT_NORM_EPSILON: f32 = 1e-5;

/// VAS1 header magic bytes.
const VAS_MAGIC: [u8; 4] = *b"VAS1";

/// VAS1 header size (16-byte aligned).
const VAS_HEADER_SIZE: usize = 16;

// ============================================================================
// In-memory VectorArena
// ============================================================================

/// Per-predicate vector arena storing packed f32 arrays.
/// Vectors are appended sequentially; each gets a u32 handle.
#[derive(Debug)]
pub struct VectorArena {
    /// Fixed dimensionality per property; 0 until first insert.
    dims: u16,
    /// Packed f32 values: values[handle * dims .. (handle+1) * dims].
    values: Vec<f32>,
    /// Number of vectors stored.
    count: u32,
    /// Whether all inserted vectors have unit norm (±epsilon).
    all_unit_norm: bool,
}

impl VectorArena {
    pub fn new() -> Self {
        Self {
            dims: 0,
            values: Vec::new(),
            count: 0,
            all_unit_norm: true,
        }
    }

    /// Insert an f32 vector, returning its handle.
    ///
    /// Non-finite values (NaN, ±Inf) are rejected. The coercion layer
    /// should have already validated this, but we check here as a safety net.
    pub fn insert_f32(&mut self, vec: &[f32]) -> Result<u32, String> {
        if vec.is_empty() {
            return Err("vector must not be empty".into());
        }
        // Reject non-finite elements
        if let Some(pos) = vec.iter().position(|v| !v.is_finite()) {
            return Err(format!(
                "vector element [{}] is not finite: {}",
                pos, vec[pos]
            ));
        }
        if self.dims == 0 {
            if vec.len() > u16::MAX as usize {
                return Err(format!("vector dims {} exceeds u16::MAX", vec.len()));
            }
            self.dims = vec.len() as u16;
        } else if vec.len() != self.dims as usize {
            return Err(format!(
                "vector dims mismatch: expected {}, got {}",
                self.dims,
                vec.len()
            ));
        }

        // Check unit norm
        if self.all_unit_norm {
            let mag2: f32 = vec.iter().map(|&x| x * x).sum();
            if (1.0 - mag2).abs() > UNIT_NORM_EPSILON {
                self.all_unit_norm = false;
            }
        }

        let handle = self.count;
        self.values.extend_from_slice(vec);
        self.count += 1;
        Ok(handle)
    }

    /// Insert an f64 vector by downcasting to f32, returning its handle.
    pub fn insert_f64(&mut self, vec: &[f64]) -> Result<u32, String> {
        let f32_vec: Vec<f32> = vec.iter().map(|&x| x as f32).collect();
        self.insert_f32(&f32_vec)
    }

    /// Borrow an f32 slice for a given handle (zero-copy hot path).
    pub fn get_f32(&self, handle: u32) -> Option<&[f32]> {
        if handle >= self.count || self.dims == 0 {
            return None;
        }
        let d = self.dims as usize;
        let start = handle as usize * d;
        let end = start + d;
        Some(&self.values[start..end])
    }

    /// Get a vector as f64 (upcast from f32). Used for FlakeValue::Vector.
    pub fn get_f64(&self, handle: u32) -> Option<Vec<f64>> {
        self.get_f32(handle)
            .map(|s| s.iter().map(|&x| x as f64).collect())
    }

    /// Dimensionality (0 if no vectors inserted yet).
    pub fn dims(&self) -> u16 {
        self.dims
    }

    /// Number of vectors stored.
    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether all stored vectors have unit norm.
    pub fn is_normalized(&self) -> bool {
        self.all_unit_norm && self.count > 0
    }

    /// Raw packed values (for persistence).
    pub fn raw_values(&self) -> &[f32] {
        &self.values
    }
}

impl Default for VectorArena {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VectorShard — parsed VAS1 data (one shard's worth of vectors)
// ============================================================================

/// Parsed VAS1 shard data.
pub struct VectorShard {
    pub dims: u16,
    pub count: u32,
    pub values: Vec<f32>,
}

impl VectorShard {
    /// Get an f32 slice for an offset within this shard.
    pub fn get_f32(&self, offset: u32) -> Option<&[f32]> {
        if offset >= self.count || self.dims == 0 {
            return None;
        }
        let d = self.dims as usize;
        let start = offset as usize * d;
        let end = start + d;
        Some(&self.values[start..end])
    }
}

// ============================================================================
// VectorManifest (VAM1) — JSON metadata
// ============================================================================

/// JSON manifest for a per-predicate vector arena.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorManifest {
    pub version: u32,
    pub dims: u16,
    pub dtype: String,
    pub normalized: bool,
    pub shard_capacity: u32,
    pub total_count: u32,
    pub shards: Vec<ShardInfo>,
}

/// One shard entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub cas: String,
    pub count: u32,
}

// ============================================================================
// VAS1 binary persistence
// ============================================================================

/// Write a single VAS1 shard from a slice of the arena.
fn write_vas1_shard(writer: &mut impl io::Write, dims: u16, data: &[f32]) -> io::Result<()> {
    let count = data.len() / dims as usize;

    // 16-byte header
    writer.write_all(&VAS_MAGIC)?;
    writer.write_all(&[1u8])?; // version
    writer.write_all(&dims.to_le_bytes())?;
    writer.write_all(&(count as u32).to_le_bytes())?;
    writer.write_all(&[0u8; 5])?; // padding to 16 bytes

    // Data: packed f32 LE
    // f32 is already LE on little-endian targets; for portability, write each value.
    for &val in data {
        writer.write_all(&val.to_le_bytes())?;
    }

    Ok(())
}

/// Write vector arena shards to disk. Returns paths of created shard files.
pub fn write_vector_shards(
    dir: &Path,
    p_id: u32,
    arena: &VectorArena,
) -> io::Result<Vec<std::path::PathBuf>> {
    if arena.is_empty() {
        return Ok(Vec::new());
    }

    let dims = arena.dims() as usize;
    let cap = SHARD_CAPACITY as usize;
    let total = arena.len() as usize;
    let num_shards = total.div_ceil(cap);
    let mut paths = Vec::with_capacity(num_shards);

    for shard_idx in 0..num_shards {
        let start_vec = shard_idx * cap;
        let end_vec = (start_vec + cap).min(total);
        let start_f32 = start_vec * dims;
        let end_f32 = end_vec * dims;
        let shard_data = &arena.raw_values()[start_f32..end_f32];

        let path = dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
        let file = std::fs::File::create(&path)?;
        let mut writer = io::BufWriter::new(file);
        write_vas1_shard(&mut writer, arena.dims(), shard_data)?;
        use io::Write;
        writer.flush()?;
        paths.push(path);
    }

    Ok(paths)
}

/// Write a vector arena manifest (VAM1 JSON).
pub fn write_vector_manifest(
    path: &Path,
    arena: &VectorArena,
    shard_addrs: &[ShardInfo],
) -> io::Result<()> {
    let manifest = VectorManifest {
        version: 1,
        dims: arena.dims(),
        dtype: "f32".to_string(),
        normalized: arena.is_normalized(),
        shard_capacity: SHARD_CAPACITY,
        total_count: arena.len(),
        shards: shard_addrs.to_vec(),
    };
    let json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Parse a VAS1 shard from bytes.
pub fn read_vector_shard_from_bytes(data: &[u8]) -> io::Result<VectorShard> {
    if data.len() < VAS_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "vector shard too small for header",
        ));
    }
    if data[0..4] != VAS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vector shard: invalid magic {:?}", &data[0..4]),
        ));
    }
    let version = data[4];
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vector shard: unsupported version {}", version),
        ));
    }
    let dims = u16::from_le_bytes(data[5..7].try_into().unwrap());
    let count = u32::from_le_bytes(data[7..11].try_into().unwrap());

    let expected_data_size = count as usize * dims as usize * 4;
    let actual_data_size = data.len() - VAS_HEADER_SIZE;
    if actual_data_size < expected_data_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "vector shard: data truncated (expected {} bytes, got {})",
                expected_data_size, actual_data_size
            ),
        ));
    }

    let mut values = Vec::with_capacity(count as usize * dims as usize);
    let data_start = VAS_HEADER_SIZE;
    for i in 0..(count as usize * dims as usize) {
        let offset = data_start + i * 4;
        let val = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        values.push(val);
    }

    Ok(VectorShard {
        dims,
        count,
        values,
    })
}

/// Parse a VAS1 shard from a file.
pub fn read_vector_shard(path: &Path) -> io::Result<VectorShard> {
    read_vector_shard_from_bytes(&std::fs::read(path)?)
}

/// Parse a VAM1 manifest from bytes.
pub fn read_vector_manifest(data: &[u8]) -> io::Result<VectorManifest> {
    serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Reassemble a VectorArena from a manifest and shard data.
pub fn load_arena_from_shards(
    manifest: &VectorManifest,
    shard_data: Vec<VectorShard>,
) -> io::Result<VectorArena> {
    let mut arena = VectorArena::new();
    if manifest.total_count == 0 {
        return Ok(arena);
    }

    // Pre-allocate
    let total_floats = manifest.total_count as usize * manifest.dims as usize;
    arena.dims = manifest.dims;
    arena.values.reserve(total_floats);
    arena.all_unit_norm = manifest.normalized;

    for shard in &shard_data {
        if shard.dims != manifest.dims {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shard dims {} != manifest dims {}",
                    shard.dims, manifest.dims
                ),
            ));
        }
        arena.values.extend_from_slice(&shard.values);
        arena.count += shard.count;
    }

    if arena.count != manifest.total_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "total count mismatch: shards have {}, manifest says {}",
                arena.count, manifest.total_count
            ),
        ));
    }

    Ok(arena)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arena() {
        let arena = VectorArena::new();
        assert_eq!(arena.len(), 0);
        assert!(arena.is_empty());
        assert_eq!(arena.dims(), 0);
        assert!(!arena.is_normalized()); // empty is not "normalized"
        assert!(arena.get_f32(0).is_none());
    }

    #[test]
    fn test_insert_and_retrieve_f32() {
        let mut arena = VectorArena::new();
        let v = vec![1.0f32, 2.0, 3.0];
        let h = arena.insert_f32(&v).unwrap();
        assert_eq!(h, 0);
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.dims(), 3);
        assert_eq!(arena.get_f32(0).unwrap(), &[1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_insert_f64_downcasts() {
        let mut arena = VectorArena::new();
        let v = vec![1.0f64, 2.0, 3.0];
        let h = arena.insert_f64(&v).unwrap();
        assert_eq!(h, 0);
        let slice = arena.get_f32(0).unwrap();
        assert_eq!(slice, &[1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_dims_mismatch_rejected() {
        let mut arena = VectorArena::new();
        arena.insert_f32(&[1.0, 2.0, 3.0]).unwrap();
        let err = arena.insert_f32(&[1.0, 2.0]).unwrap_err();
        assert!(err.contains("dims mismatch"));
    }

    #[test]
    fn test_normalized_tracking() {
        let mut arena = VectorArena::new();
        // Unit vector in 3D
        let inv_sqrt3 = 1.0f32 / 3.0f32.sqrt();
        arena
            .insert_f32(&[inv_sqrt3, inv_sqrt3, inv_sqrt3])
            .unwrap();
        assert!(arena.is_normalized());

        // Non-unit vector
        arena.insert_f32(&[2.0, 0.0, 0.0]).unwrap();
        assert!(!arena.is_normalized());
    }

    #[test]
    fn test_sequential_handles() {
        let mut arena = VectorArena::new();
        assert_eq!(arena.insert_f32(&[1.0, 0.0]).unwrap(), 0);
        assert_eq!(arena.insert_f32(&[0.0, 1.0]).unwrap(), 1);
        assert_eq!(arena.insert_f32(&[1.0, 1.0]).unwrap(), 2);
        assert_eq!(arena.len(), 3);
    }

    #[test]
    fn test_get_f64_upcasts() {
        let mut arena = VectorArena::new();
        arena.insert_f32(&[1.5, 2.5]).unwrap();
        let v = arena.get_f64(0).unwrap();
        assert!((v[0] - 1.5f64).abs() < 1e-6);
        assert!((v[1] - 2.5f64).abs() < 1e-6);
    }

    #[test]
    fn test_vas1_round_trip() {
        let mut arena = VectorArena::new();
        for i in 0..5 {
            arena
                .insert_f32(&[i as f32, (i * 2) as f32, (i * 3) as f32])
                .unwrap();
        }

        let dir = std::env::temp_dir().join("fluree_vas1_test");
        let _ = std::fs::create_dir_all(&dir);

        let paths = write_vector_shards(&dir, 42, &arena).unwrap();
        assert_eq!(paths.len(), 1); // 5 vectors < SHARD_CAPACITY

        let shard = read_vector_shard(&paths[0]).unwrap();
        assert_eq!(shard.dims, 3);
        assert_eq!(shard.count, 5);
        assert_eq!(shard.get_f32(0).unwrap(), &[0.0f32, 0.0, 0.0]);
        assert_eq!(shard.get_f32(4).unwrap(), &[4.0f32, 8.0, 12.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_manifest_round_trip() {
        let mut arena = VectorArena::new();
        let inv_sqrt2 = 1.0f32 / 2.0f32.sqrt();
        arena.insert_f32(&[inv_sqrt2, inv_sqrt2]).unwrap();
        arena.insert_f32(&[inv_sqrt2, -inv_sqrt2]).unwrap();

        let shard_infos = vec![ShardInfo {
            cas: "fluree:file://test".to_string(),
            count: 2,
        }];

        let dir = std::env::temp_dir().join("fluree_vam1_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.vam");

        write_vector_manifest(&path, &arena, &shard_infos).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let manifest = read_vector_manifest(&bytes).unwrap();

        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.dims, 2);
        assert_eq!(manifest.dtype, "f32");
        assert!(manifest.normalized);
        assert_eq!(manifest.total_count, 2);
        assert_eq!(manifest.shards.len(), 1);
        assert_eq!(manifest.shards[0].count, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_arena_from_shards() {
        let mut arena = VectorArena::new();
        for i in 0..5u32 {
            arena.insert_f32(&[i as f32, (i + 1) as f32]).unwrap();
        }

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 5,
            shards: vec![ShardInfo {
                cas: "test".to_string(),
                count: 5,
            }],
        };

        let shard = VectorShard {
            dims: 2,
            count: 5,
            values: arena.raw_values().to_vec(),
        };

        let loaded = load_arena_from_shards(&manifest, vec![shard]).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded.dims(), 2);
        assert_eq!(loaded.get_f32(0).unwrap(), &[0.0f32, 1.0]);
        assert_eq!(loaded.get_f32(4).unwrap(), &[4.0f32, 5.0]);
    }

    #[test]
    fn test_nan_rejected() {
        let mut arena = VectorArena::new();
        let err = arena.insert_f32(&[1.0, f32::NAN, 3.0]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");
    }

    #[test]
    fn test_infinity_rejected() {
        let mut arena = VectorArena::new();
        let err = arena.insert_f32(&[f32::INFINITY, 0.0]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");

        let mut arena2 = VectorArena::new();
        let err2 = arena2.insert_f32(&[0.0, f32::NEG_INFINITY]).unwrap_err();
        assert!(err2.contains("not finite"), "got: {err2}");
    }

    #[test]
    fn test_f64_overflow_rejected_via_insert_f64() {
        let mut arena = VectorArena::new();
        // f64::MAX overflows to f32::INFINITY
        let err = arena.insert_f64(&[1.0, f64::MAX]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");
    }
}
