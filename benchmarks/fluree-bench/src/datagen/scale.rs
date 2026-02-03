//! Scale calculation: convert a target MB size into a unit count.

/// Compute how many units to generate for a given target data size.
///
/// Generates a single sample unit, measures its serialized JSON byte size,
/// and computes `ceil(target_bytes / bytes_per_unit)`.
pub fn compute_units(target_mb: u32) -> usize {
    let sample = super::supply_chain::generate_unit(0);
    let sample_bytes = serde_json::to_string(&sample)
        .expect("sample serialization must succeed")
        .len();

    let target_bytes = target_mb as usize * 1_048_576;
    if sample_bytes == 0 {
        return 1;
    }
    target_bytes.div_ceil(sample_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_units_positive() {
        let units = compute_units(1);
        assert!(units >= 1, "Should produce at least 1 unit for 1 MB");
    }

    #[test]
    fn test_compute_units_scales() {
        let u1 = compute_units(1);
        let u10 = compute_units(10);
        assert!(
            u10 >= u1 * 5,
            "10 MB should produce substantially more units than 1 MB (u1={u1}, u10={u10})"
        );
    }
}
