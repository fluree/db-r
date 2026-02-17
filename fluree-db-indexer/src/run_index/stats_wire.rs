//! Binary wire format for index stats and schema sections.
//!
//! These are embedded in the `IndexRootV5` binary root (not separate blobs).
//! The encodings are fully structured binary — no JSON anywhere.
//!
//! ## Stats wire format
//!
//! See `encode_stats()` / `decode_stats()`. Determinism invariants:
//! - Graphs sorted by `g_id`, properties by `p_id`
//! - Aggregate properties sorted by `(ns_code, suffix)`
//! - Classes sorted by `(ns_code, suffix)`, properties within classes likewise
//!
//! ## Schema wire format
//!
//! See `encode_schema()` / `decode_schema()`. Determinism invariants:
//! - Entries sorted by `(ns_code, suffix)` (Sid ordering)

use fluree_db_core::index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
use fluree_db_core::index_stats::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry,
    IndexStats, PropertyStatEntry,
};
use fluree_db_core::sid::Sid;
use std::io;

// ============================================================================
// Shared helpers: Sid wire encoding
// ============================================================================

/// Encode a `Sid` as `(ns_code: u16 LE, suffix_len: u16 LE, suffix_bytes)`.
fn write_sid(buf: &mut Vec<u8>, sid: &Sid) {
    buf.extend_from_slice(&sid.namespace_code.to_le_bytes());
    let name_bytes = sid.name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(name_bytes);
}

/// Encode a `(ns_code, suffix)` tuple the same way.
fn write_sid_tuple(buf: &mut Vec<u8>, ns_code: u16, suffix: &str) {
    buf.extend_from_slice(&ns_code.to_le_bytes());
    let suffix_bytes = suffix.as_bytes();
    buf.extend_from_slice(&(suffix_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(suffix_bytes);
}

/// Decode a Sid from wire format. Returns `(sid, bytes_consumed)`.
fn read_sid(data: &[u8], pos: usize) -> io::Result<(Sid, usize)> {
    let mut p = pos;
    ensure_len(data, p, 4, "sid header")?;
    let ns_code = u16::from_le_bytes(data[p..p + 2].try_into().unwrap());
    p += 2;
    let suffix_len = u16::from_le_bytes(data[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    ensure_len(data, p, suffix_len, "sid suffix")?;
    let suffix = std::str::from_utf8(&data[p..p + suffix_len]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in sid: {e}"),
        )
    })?;
    p += suffix_len;
    Ok((Sid::new(ns_code, suffix), p))
}

/// Decode a `(ns_code, suffix_string)` tuple. Returns `((ns_code, suffix), bytes_consumed)`.
fn read_sid_tuple(data: &[u8], pos: usize) -> io::Result<((u16, String), usize)> {
    let mut p = pos;
    ensure_len(data, p, 4, "sid tuple header")?;
    let ns_code = u16::from_le_bytes(data[p..p + 2].try_into().unwrap());
    p += 2;
    let suffix_len = u16::from_le_bytes(data[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    ensure_len(data, p, suffix_len, "sid tuple suffix")?;
    let suffix = std::str::from_utf8(&data[p..p + suffix_len]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in sid tuple: {e}"),
        )
    })?;
    p += suffix_len;
    Ok(((ns_code, suffix.to_string()), p))
}

/// Check that `data[pos..pos+need]` is within bounds.
#[inline]
fn ensure_len(data: &[u8], pos: usize, need: usize, ctx: &str) -> io::Result<()> {
    if pos + need > data.len() {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stats/schema: truncated at {ctx} (need {need} bytes at offset {pos}, have {})",
                data.len()
            ),
        ))
    } else {
        Ok(())
    }
}

/// Read a u8 at `pos`, advancing.
#[inline]
fn read_u8(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    ensure_len(data, *pos, 1, "u8")?;
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

/// Read a u16 LE at `pos`, advancing.
#[inline]
fn read_u16(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    ensure_len(data, *pos, 2, "u16")?;
    let v = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

/// Read a u32 LE at `pos`, advancing.
#[inline]
fn read_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    ensure_len(data, *pos, 4, "u32")?;
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

/// Read a u64 LE at `pos`, advancing.
#[inline]
fn read_u64(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    ensure_len(data, *pos, 8, "u64")?;
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Read an i64 LE at `pos`, advancing.
#[inline]
fn read_i64(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    ensure_len(data, *pos, 8, "i64")?;
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

// ============================================================================
// Stats encode
// ============================================================================

/// Encode `IndexStats` to the binary stats section wire format.
///
/// Determinism: graphs sorted by g_id, properties by p_id, aggregate
/// properties by (ns_code, suffix), classes by (ns_code, suffix).
pub fn encode_stats(stats: &IndexStats) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    // Top-level aggregates
    buf.extend_from_slice(&stats.flakes.to_le_bytes());
    buf.extend_from_slice(&stats.size.to_le_bytes());

    // Per-graph stats
    let graphs = stats.graphs.as_deref().unwrap_or(&[]);
    let mut sorted_graphs: Vec<&GraphStatsEntry> = graphs.iter().collect();
    sorted_graphs.sort_by_key(|g| g.g_id);

    buf.extend_from_slice(&(sorted_graphs.len() as u16).to_le_bytes());
    for g in &sorted_graphs {
        buf.extend_from_slice(&g.g_id.to_le_bytes());
        buf.extend_from_slice(&g.flakes.to_le_bytes());
        buf.extend_from_slice(&g.size.to_le_bytes());

        let mut sorted_props: Vec<&GraphPropertyStatEntry> = g.properties.iter().collect();
        sorted_props.sort_by_key(|p| p.p_id);
        buf.extend_from_slice(&(sorted_props.len() as u32).to_le_bytes());
        for p in &sorted_props {
            encode_graph_property(&mut buf, p);
        }
    }

    // Aggregate properties (SID-keyed)
    let agg_props = stats.properties.as_deref().unwrap_or(&[]);
    let mut sorted_agg: Vec<&PropertyStatEntry> = agg_props.iter().collect();
    sorted_agg.sort_by(|a, b| a.sid.0.cmp(&b.sid.0).then_with(|| a.sid.1.cmp(&b.sid.1)));

    buf.extend_from_slice(&(sorted_agg.len() as u32).to_le_bytes());
    for p in &sorted_agg {
        write_sid_tuple(&mut buf, p.sid.0, &p.sid.1);
        buf.extend_from_slice(&p.count.to_le_bytes());
        buf.extend_from_slice(&p.ndv_values.to_le_bytes());
        buf.extend_from_slice(&p.ndv_subjects.to_le_bytes());
        buf.extend_from_slice(&p.last_modified_t.to_le_bytes());
        encode_datatypes(&mut buf, &p.datatypes);
    }

    // Classes
    let classes = stats.classes.as_deref().unwrap_or(&[]);
    let mut sorted_classes: Vec<&ClassStatEntry> = classes.iter().collect();
    sorted_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

    buf.extend_from_slice(&(sorted_classes.len() as u32).to_le_bytes());
    for c in &sorted_classes {
        write_sid(&mut buf, &c.class_sid);
        buf.extend_from_slice(&c.count.to_le_bytes());

        let mut sorted_props: Vec<&ClassPropertyUsage> = c.properties.iter().collect();
        sorted_props.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));

        buf.extend_from_slice(&(sorted_props.len() as u16).to_le_bytes());
        for pu in &sorted_props {
            write_sid(&mut buf, &pu.property_sid);

            let mut sorted_refs: Vec<&ClassRefCount> = pu.ref_classes.iter().collect();
            sorted_refs.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

            buf.extend_from_slice(&(sorted_refs.len() as u16).to_le_bytes());
            for rc in &sorted_refs {
                write_sid(&mut buf, &rc.class_sid);
                buf.extend_from_slice(&rc.count.to_le_bytes());
            }
        }
    }

    buf
}

fn encode_graph_property(buf: &mut Vec<u8>, p: &GraphPropertyStatEntry) {
    buf.extend_from_slice(&p.p_id.to_le_bytes());
    buf.extend_from_slice(&p.count.to_le_bytes());
    buf.extend_from_slice(&p.ndv_values.to_le_bytes());
    buf.extend_from_slice(&p.ndv_subjects.to_le_bytes());
    buf.extend_from_slice(&p.last_modified_t.to_le_bytes());
    encode_datatypes(buf, &p.datatypes);
}

fn encode_datatypes(buf: &mut Vec<u8>, datatypes: &[(u8, u64)]) {
    buf.push(datatypes.len() as u8);
    for &(dt_tag, dt_count) in datatypes {
        buf.push(dt_tag);
        buf.extend_from_slice(&dt_count.to_le_bytes());
    }
}

// ============================================================================
// Stats decode
// ============================================================================

/// Decode `IndexStats` from the binary stats section wire format.
pub fn decode_stats(data: &[u8]) -> io::Result<IndexStats> {
    let mut pos = 0usize;

    let flakes = read_u64(data, &mut pos)?;
    let size = read_u64(data, &mut pos)?;

    // Per-graph stats
    let graph_count = read_u16(data, &mut pos)? as usize;
    let mut graphs = Vec::with_capacity(graph_count);
    for _ in 0..graph_count {
        let g_id = read_u16(data, &mut pos)?;
        let g_flakes = read_u64(data, &mut pos)?;
        let g_size = read_u64(data, &mut pos)?;

        let prop_count = read_u32(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(prop_count);
        for _ in 0..prop_count {
            properties.push(decode_graph_property(data, &mut pos)?);
        }

        graphs.push(GraphStatsEntry {
            g_id,
            flakes: g_flakes,
            size: g_size,
            properties,
        });
    }

    // Aggregate properties
    let agg_count = read_u32(data, &mut pos)? as usize;
    let mut agg_props = Vec::with_capacity(agg_count);
    for _ in 0..agg_count {
        let (sid, new_pos) = read_sid_tuple(data, pos)?;
        pos = new_pos;
        let count = read_u64(data, &mut pos)?;
        let ndv_values = read_u64(data, &mut pos)?;
        let ndv_subjects = read_u64(data, &mut pos)?;
        let last_modified_t = read_i64(data, &mut pos)?;
        let datatypes = decode_datatypes(data, &mut pos)?;

        agg_props.push(PropertyStatEntry {
            sid,
            count,
            ndv_values,
            ndv_subjects,
            last_modified_t,
            datatypes,
        });
    }

    // Classes
    let class_count = read_u32(data, &mut pos)? as usize;
    let mut classes = Vec::with_capacity(class_count);
    for _ in 0..class_count {
        let (class_sid, new_pos) = read_sid(data, pos)?;
        pos = new_pos;
        let instance_count = read_u64(data, &mut pos)?;

        let pu_count = read_u16(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(pu_count);
        for _ in 0..pu_count {
            let (property_sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;

            let rc_count = read_u16(data, &mut pos)? as usize;
            let mut ref_classes = Vec::with_capacity(rc_count);
            for _ in 0..rc_count {
                let (ref_sid, new_pos3) = read_sid(data, pos)?;
                pos = new_pos3;
                let ref_count = read_u64(data, &mut pos)?;
                ref_classes.push(ClassRefCount {
                    class_sid: ref_sid,
                    count: ref_count,
                });
            }

            properties.push(ClassPropertyUsage {
                property_sid,
                ref_classes,
            });
        }

        classes.push(ClassStatEntry {
            class_sid,
            count: instance_count,
            properties,
        });
    }

    Ok(IndexStats {
        flakes,
        size,
        properties: if agg_props.is_empty() {
            None
        } else {
            Some(agg_props)
        },
        classes: if classes.is_empty() {
            None
        } else {
            Some(classes)
        },
        graphs: if graphs.is_empty() {
            None
        } else {
            Some(graphs)
        },
    })
}

fn decode_graph_property(data: &[u8], pos: &mut usize) -> io::Result<GraphPropertyStatEntry> {
    let p_id = read_u32(data, pos)?;
    let count = read_u64(data, pos)?;
    let ndv_values = read_u64(data, pos)?;
    let ndv_subjects = read_u64(data, pos)?;
    let last_modified_t = read_i64(data, pos)?;
    let datatypes = decode_datatypes(data, pos)?;

    Ok(GraphPropertyStatEntry {
        p_id,
        count,
        ndv_values,
        ndv_subjects,
        last_modified_t,
        datatypes,
    })
}

fn decode_datatypes(data: &[u8], pos: &mut usize) -> io::Result<Vec<(u8, u64)>> {
    let count = read_u8(data, pos)? as usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let dt_tag = read_u8(data, pos)?;
        let dt_count = read_u64(data, pos)?;
        result.push((dt_tag, dt_count));
    }
    Ok(result)
}

// ============================================================================
// Schema encode
// ============================================================================

/// Encode `IndexSchema` to the binary schema section wire format.
///
/// Sids are encoded as `(ns_code: u16, suffix_len: u16, suffix_bytes)`.
/// Entries are sorted by Sid ordering (ns_code, then suffix).
pub fn encode_schema(schema: &IndexSchema) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    buf.extend_from_slice(&schema.t.to_le_bytes());

    let mut sorted_entries: Vec<&SchemaPredicateInfo> = schema.pred.vals.iter().collect();
    sorted_entries.sort_by(|a, b| a.id.cmp(&b.id));

    buf.extend_from_slice(&(sorted_entries.len() as u32).to_le_bytes());
    for entry in &sorted_entries {
        write_sid(&mut buf, &entry.id);

        // subclass_of
        let mut sorted_sc: Vec<&Sid> = entry.subclass_of.iter().collect();
        sorted_sc.sort();
        buf.extend_from_slice(&(sorted_sc.len() as u16).to_le_bytes());
        for sid in &sorted_sc {
            write_sid(&mut buf, sid);
        }

        // parent_props
        let mut sorted_pp: Vec<&Sid> = entry.parent_props.iter().collect();
        sorted_pp.sort();
        buf.extend_from_slice(&(sorted_pp.len() as u16).to_le_bytes());
        for sid in &sorted_pp {
            write_sid(&mut buf, sid);
        }

        // child_props
        let mut sorted_cp: Vec<&Sid> = entry.child_props.iter().collect();
        sorted_cp.sort();
        buf.extend_from_slice(&(sorted_cp.len() as u16).to_le_bytes());
        for sid in &sorted_cp {
            write_sid(&mut buf, sid);
        }
    }

    buf
}

// ============================================================================
// Schema decode
// ============================================================================

/// Decode `IndexSchema` from the binary schema section wire format.
pub fn decode_schema(data: &[u8]) -> io::Result<IndexSchema> {
    let mut pos = 0usize;

    let t = read_i64(data, &mut pos)?;
    let entry_count = read_u32(data, &mut pos)? as usize;

    let mut vals = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (id, new_pos) = read_sid(data, pos)?;
        pos = new_pos;

        // subclass_of
        let sc_count = read_u16(data, &mut pos)? as usize;
        let mut subclass_of = Vec::with_capacity(sc_count);
        for _ in 0..sc_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            subclass_of.push(sid);
        }

        // parent_props
        let pp_count = read_u16(data, &mut pos)? as usize;
        let mut parent_props = Vec::with_capacity(pp_count);
        for _ in 0..pp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            parent_props.push(sid);
        }

        // child_props
        let cp_count = read_u16(data, &mut pos)? as usize;
        let mut child_props = Vec::with_capacity(cp_count);
        for _ in 0..cp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            child_props.push(sid);
        }

        vals.push(SchemaPredicateInfo {
            id,
            subclass_of,
            parent_props,
            child_props,
        });
    }

    Ok(IndexSchema {
        t,
        pred: SchemaPredicates {
            keys: vec![
                "id".to_string(),
                "subclassOf".to_string(),
                "parentProps".to_string(),
                "childProps".to_string(),
            ],
            vals,
        },
    })
}

// ============================================================================
// Public helpers for root encoder
// ============================================================================

/// Returns the number of bytes consumed when reading stats from a slice.
/// Used by the root decoder to know where the stats section ends.
pub fn decode_stats_with_len(data: &[u8]) -> io::Result<(IndexStats, usize)> {
    // We decode the stats, tracking position manually.
    // This is a wrapper that re-decodes to get the final position.
    // For efficiency, we implement a position-tracking decode.
    let mut pos = 0usize;

    let flakes = read_u64(data, &mut pos)?;
    let size = read_u64(data, &mut pos)?;

    let graph_count = read_u16(data, &mut pos)? as usize;
    let mut graphs = Vec::with_capacity(graph_count);
    for _ in 0..graph_count {
        let g_id = read_u16(data, &mut pos)?;
        let g_flakes = read_u64(data, &mut pos)?;
        let g_size = read_u64(data, &mut pos)?;
        let prop_count = read_u32(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(prop_count);
        for _ in 0..prop_count {
            properties.push(decode_graph_property(data, &mut pos)?);
        }
        graphs.push(GraphStatsEntry {
            g_id,
            flakes: g_flakes,
            size: g_size,
            properties,
        });
    }

    let agg_count = read_u32(data, &mut pos)? as usize;
    let mut agg_props = Vec::with_capacity(agg_count);
    for _ in 0..agg_count {
        let (sid, new_pos) = read_sid_tuple(data, pos)?;
        pos = new_pos;
        let count = read_u64(data, &mut pos)?;
        let ndv_values = read_u64(data, &mut pos)?;
        let ndv_subjects = read_u64(data, &mut pos)?;
        let last_modified_t = read_i64(data, &mut pos)?;
        let datatypes = decode_datatypes(data, &mut pos)?;
        agg_props.push(PropertyStatEntry {
            sid,
            count,
            ndv_values,
            ndv_subjects,
            last_modified_t,
            datatypes,
        });
    }

    let class_count = read_u32(data, &mut pos)? as usize;
    let mut classes = Vec::with_capacity(class_count);
    for _ in 0..class_count {
        let (class_sid, new_pos) = read_sid(data, pos)?;
        pos = new_pos;
        let instance_count = read_u64(data, &mut pos)?;
        let pu_count = read_u16(data, &mut pos)? as usize;
        let mut properties = Vec::with_capacity(pu_count);
        for _ in 0..pu_count {
            let (property_sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            let rc_count = read_u16(data, &mut pos)? as usize;
            let mut ref_classes = Vec::with_capacity(rc_count);
            for _ in 0..rc_count {
                let (ref_sid, new_pos3) = read_sid(data, pos)?;
                pos = new_pos3;
                let ref_count = read_u64(data, &mut pos)?;
                ref_classes.push(ClassRefCount {
                    class_sid: ref_sid,
                    count: ref_count,
                });
            }
            properties.push(ClassPropertyUsage {
                property_sid,
                ref_classes,
            });
        }
        classes.push(ClassStatEntry {
            class_sid,
            count: instance_count,
            properties,
        });
    }

    let stats = IndexStats {
        flakes,
        size,
        properties: if agg_props.is_empty() {
            None
        } else {
            Some(agg_props)
        },
        classes: if classes.is_empty() {
            None
        } else {
            Some(classes)
        },
        graphs: if graphs.is_empty() {
            None
        } else {
            Some(graphs)
        },
    };

    Ok((stats, pos))
}

/// Decode schema and return bytes consumed.
pub fn decode_schema_with_len(data: &[u8]) -> io::Result<(IndexSchema, usize)> {
    let mut pos = 0usize;

    let t = read_i64(data, &mut pos)?;
    let entry_count = read_u32(data, &mut pos)? as usize;

    let mut vals = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (id, new_pos) = read_sid(data, pos)?;
        pos = new_pos;

        let sc_count = read_u16(data, &mut pos)? as usize;
        let mut subclass_of = Vec::with_capacity(sc_count);
        for _ in 0..sc_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            subclass_of.push(sid);
        }

        let pp_count = read_u16(data, &mut pos)? as usize;
        let mut parent_props = Vec::with_capacity(pp_count);
        for _ in 0..pp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            parent_props.push(sid);
        }

        let cp_count = read_u16(data, &mut pos)? as usize;
        let mut child_props = Vec::with_capacity(cp_count);
        for _ in 0..cp_count {
            let (sid, new_pos2) = read_sid(data, pos)?;
            pos = new_pos2;
            child_props.push(sid);
        }

        vals.push(SchemaPredicateInfo {
            id,
            subclass_of,
            parent_props,
            child_props,
        });
    }

    let schema = IndexSchema {
        t,
        pred: SchemaPredicates {
            keys: vec![
                "id".to_string(),
                "subclassOf".to_string(),
                "parentProps".to_string(),
                "childProps".to_string(),
            ],
            vals,
        },
    };

    Ok((schema, pos))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    // ---- Stats tests ----

    #[test]
    fn test_stats_empty_round_trip() {
        let stats = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        assert_eq!(decoded.flakes, 0);
        assert_eq!(decoded.size, 0);
        assert!(decoded.properties.is_none());
        assert!(decoded.classes.is_none());
        assert!(decoded.graphs.is_none());
    }

    #[test]
    fn test_stats_with_graphs_round_trip() {
        let stats = IndexStats {
            flakes: 50_000,
            size: 1_000_000,
            properties: None,
            classes: None,
            graphs: Some(vec![
                GraphStatsEntry {
                    g_id: 0,
                    flakes: 40_000,
                    size: 800_000,
                    properties: vec![
                        GraphPropertyStatEntry {
                            p_id: 1,
                            count: 10_000,
                            ndv_values: 5_000,
                            ndv_subjects: 2_000,
                            last_modified_t: 42,
                            datatypes: vec![(3, 8_000), (7, 2_000)],
                        },
                        GraphPropertyStatEntry {
                            p_id: 5,
                            count: 30_000,
                            ndv_values: 15_000,
                            ndv_subjects: 10_000,
                            last_modified_t: 100,
                            datatypes: vec![(1, 30_000)],
                        },
                    ],
                },
                GraphStatsEntry {
                    g_id: 1,
                    flakes: 10_000,
                    size: 200_000,
                    properties: vec![],
                },
            ]),
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        assert_eq!(decoded.flakes, 50_000);
        assert_eq!(decoded.size, 1_000_000);
        let graphs = decoded.graphs.unwrap();
        assert_eq!(graphs.len(), 2);
        assert_eq!(graphs[0].g_id, 0);
        assert_eq!(graphs[0].properties.len(), 2);
        assert_eq!(graphs[0].properties[0].p_id, 1);
        assert_eq!(graphs[0].properties[0].datatypes.len(), 2);
        assert_eq!(graphs[0].properties[1].p_id, 5);
        assert_eq!(graphs[1].g_id, 1);
        assert_eq!(graphs[1].properties.len(), 0);
    }

    #[test]
    fn test_stats_with_agg_properties_round_trip() {
        let stats = IndexStats {
            flakes: 100,
            size: 500,
            properties: Some(vec![
                PropertyStatEntry {
                    sid: (10, "name".to_string()),
                    count: 50,
                    ndv_values: 45,
                    ndv_subjects: 50,
                    last_modified_t: 3,
                    datatypes: vec![(1, 50)],
                },
                PropertyStatEntry {
                    sid: (10, "age".to_string()),
                    count: 50,
                    ndv_values: 30,
                    ndv_subjects: 50,
                    last_modified_t: 3,
                    datatypes: vec![(3, 50)],
                },
            ]),
            classes: None,
            graphs: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        let props = decoded.properties.unwrap();
        assert_eq!(props.len(), 2);
        // Sorted by (ns_code, suffix): "age" < "name"
        assert_eq!(props[0].sid.1, "age");
        assert_eq!(props[1].sid.1, "name");
    }

    #[test]
    fn test_stats_with_classes_round_trip() {
        let stats = IndexStats {
            flakes: 200,
            size: 1000,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: sid(5, "Person"),
                count: 100,
                properties: vec![
                    ClassPropertyUsage {
                        property_sid: sid(5, "name"),
                        ref_classes: vec![],
                    },
                    ClassPropertyUsage {
                        property_sid: sid(5, "knows"),
                        ref_classes: vec![
                            ClassRefCount {
                                class_sid: sid(5, "Person"),
                                count: 80,
                            },
                            ClassRefCount {
                                class_sid: sid(5, "Organization"),
                                count: 20,
                            },
                        ],
                    },
                ],
            }]),
            graphs: None,
        };

        let bytes = encode_stats(&stats);
        let decoded = decode_stats(&bytes).unwrap();

        let classes = decoded.classes.unwrap();
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].class_sid, sid(5, "Person"));
        assert_eq!(classes[0].count, 100);
        assert_eq!(classes[0].properties.len(), 2);

        // "knows" < "name" in sort order
        assert_eq!(classes[0].properties[0].property_sid, sid(5, "knows"));
        assert_eq!(classes[0].properties[0].ref_classes.len(), 2);
        // "Organization" < "Person"
        assert_eq!(
            classes[0].properties[0].ref_classes[0].class_sid,
            sid(5, "Organization")
        );
        assert_eq!(classes[0].properties[0].ref_classes[0].count, 20);
        assert_eq!(
            classes[0].properties[0].ref_classes[1].class_sid,
            sid(5, "Person")
        );
        assert_eq!(classes[0].properties[0].ref_classes[1].count, 80);

        assert_eq!(classes[0].properties[1].property_sid, sid(5, "name"));
        assert_eq!(classes[0].properties[1].ref_classes.len(), 0);
    }

    #[test]
    fn test_stats_determinism() {
        let stats = IndexStats {
            flakes: 100,
            size: 500,
            properties: Some(vec![
                PropertyStatEntry {
                    sid: (10, "zzz".to_string()),
                    count: 1,
                    ndv_values: 1,
                    ndv_subjects: 1,
                    last_modified_t: 1,
                    datatypes: vec![],
                },
                PropertyStatEntry {
                    sid: (10, "aaa".to_string()),
                    count: 2,
                    ndv_values: 2,
                    ndv_subjects: 2,
                    last_modified_t: 2,
                    datatypes: vec![],
                },
            ]),
            classes: None,
            graphs: None,
        };

        let bytes1 = encode_stats(&stats);
        let bytes2 = encode_stats(&stats);
        assert_eq!(bytes1, bytes2, "same inputs must produce identical bytes");
    }

    #[test]
    fn test_stats_with_len() {
        let stats = IndexStats {
            flakes: 42,
            size: 100,
            properties: None,
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 42,
                size: 100,
                properties: vec![],
            }]),
        };

        let bytes = encode_stats(&stats);
        let (decoded, consumed) = decode_stats_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.flakes, 42);
    }

    // ---- Schema tests ----

    #[test]
    fn test_schema_empty_round_trip() {
        let schema = IndexSchema::default();

        let bytes = encode_schema(&schema);
        let decoded = decode_schema(&bytes).unwrap();

        assert_eq!(decoded.t, 0);
        assert!(decoded.pred.vals.is_empty());
    }

    #[test]
    fn test_schema_with_entries_round_trip() {
        let schema = IndexSchema {
            t: 42,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: vec![
                    SchemaPredicateInfo {
                        id: sid(5, "Person"),
                        subclass_of: vec![sid(5, "Agent")],
                        parent_props: vec![],
                        child_props: vec![sid(5, "Employee")],
                    },
                    SchemaPredicateInfo {
                        id: sid(5, "Agent"),
                        subclass_of: vec![],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                ],
            },
        };

        let bytes = encode_schema(&schema);
        let decoded = decode_schema(&bytes).unwrap();

        assert_eq!(decoded.t, 42);
        assert_eq!(decoded.pred.vals.len(), 2);
        // Sorted by sid: Agent < Person
        assert_eq!(decoded.pred.vals[0].id, sid(5, "Agent"));
        assert_eq!(decoded.pred.vals[1].id, sid(5, "Person"));
        assert_eq!(decoded.pred.vals[1].subclass_of.len(), 1);
        assert_eq!(decoded.pred.vals[1].subclass_of[0], sid(5, "Agent"));
        assert_eq!(decoded.pred.vals[1].child_props.len(), 1);
        assert_eq!(decoded.pred.vals[1].child_props[0], sid(5, "Employee"));
    }

    #[test]
    fn test_schema_determinism() {
        let schema = IndexSchema {
            t: 10,
            pred: SchemaPredicates {
                keys: vec![],
                vals: vec![
                    SchemaPredicateInfo {
                        id: sid(5, "Z"),
                        subclass_of: vec![sid(5, "B"), sid(5, "A")],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                    SchemaPredicateInfo {
                        id: sid(5, "A"),
                        subclass_of: vec![],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                ],
            },
        };

        let bytes1 = encode_schema(&schema);
        let bytes2 = encode_schema(&schema);
        assert_eq!(bytes1, bytes2, "same inputs must produce identical bytes");

        // Also verify sorted order after decode
        let decoded = decode_schema(&bytes1).unwrap();
        assert_eq!(decoded.pred.vals[0].id, sid(5, "A"));
        assert_eq!(decoded.pred.vals[1].id, sid(5, "Z"));
        // subclass_of within Z should be sorted
        assert_eq!(decoded.pred.vals[1].subclass_of[0], sid(5, "A"));
        assert_eq!(decoded.pred.vals[1].subclass_of[1], sid(5, "B"));
    }

    #[test]
    fn test_schema_with_len() {
        let schema = IndexSchema {
            t: 5,
            pred: SchemaPredicates {
                keys: vec![],
                vals: vec![SchemaPredicateInfo {
                    id: sid(1, "test"),
                    subclass_of: vec![],
                    parent_props: vec![],
                    child_props: vec![],
                }],
            },
        };

        let bytes = encode_schema(&schema);
        let (decoded, consumed) = decode_schema_with_len(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.t, 5);
    }
}
