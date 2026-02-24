//! Fulltext collection hook for commit resolution.
//!
//! Collects `@fulltext`-typed string entries during commit resolution to build
//! fulltext BoW arenas alongside the binary index.

use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::GraphId;

/// Entry collected for fulltext indexing.
#[derive(Debug, Clone)]
pub struct FulltextEntry {
    /// Graph ID (0 = default graph).
    pub g_id: GraphId,
    /// Predicate ID in binary index space.
    pub p_id: u32,
    /// String dictionary ID (the o_key for ObjKind::LEX_ID).
    pub string_id: u32,
    /// Transaction time.
    pub t: i64,
    /// true = assertion, false = retraction.
    pub is_assert: bool,
}

/// Hook for collecting `@fulltext`-typed literals during commit resolution.
///
/// Filters by `dt_id == DatatypeDictId::FULL_TEXT` (14). The string text
/// itself is NOT copied — only the string dictionary ID is stored. The
/// arena builder retrieves text from the string dict when it needs to
/// analyze (tokenize/stem).
#[derive(Debug)]
pub struct FulltextHook {
    entries: Vec<FulltextEntry>,
}

impl FulltextHook {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Process a resolved record during commit resolution.
    ///
    /// Only collects entries where `dt_id` matches the reserved `@fulltext`
    /// datatype dict ID AND `o_kind` is `LEX_ID` (string values). This guards
    /// against non-string objects that happen to carry the `@fulltext` dt_id
    /// (e.g., via coercion rules).
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn on_op(
        &mut self,
        g_id: GraphId,
        p_id: u32,
        dt_id: u16,
        o_kind: u8,
        o_key: u64,
        t: i64,
        is_assert: bool,
    ) {
        if dt_id != DatatypeDictId::FULL_TEXT.as_u16() {
            return;
        }
        if ObjKind::from_u8(o_kind) != ObjKind::LEX_ID {
            return;
        }
        self.entries.push(FulltextEntry {
            g_id,
            p_id,
            string_id: o_key as u32,
            t,
            is_assert,
        });
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn into_entries(self) -> Vec<FulltextEntry> {
        self.entries
    }

    pub fn entries(&self) -> &[FulltextEntry] {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut [FulltextEntry] {
        &mut self.entries
    }
}

impl Default for FulltextHook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEX: u8 = ObjKind::LEX_ID.as_u8();

    #[test]
    fn test_hook_collects_fulltext() {
        let mut hook = FulltextHook::new();
        assert!(hook.is_empty());

        // Fulltext dt_id = 14, ObjKind = LEX_ID
        hook.on_op(0, 5, DatatypeDictId::FULL_TEXT.as_u16(), LEX, 42, 1, true);
        assert_eq!(hook.entry_count(), 1);
        assert_eq!(hook.entries()[0].string_id, 42);
        assert_eq!(hook.entries()[0].p_id, 5);
        assert!(hook.entries()[0].is_assert);
    }

    #[test]
    fn test_hook_skips_non_fulltext() {
        let mut hook = FulltextHook::new();

        // String dt_id = 1, not fulltext
        hook.on_op(0, 5, DatatypeDictId::STRING.as_u16(), LEX, 42, 1, true);
        assert!(hook.is_empty());

        // Vector dt_id = 13, not fulltext
        hook.on_op(0, 5, DatatypeDictId::VECTOR.as_u16(), LEX, 42, 1, true);
        assert!(hook.is_empty());
    }

    #[test]
    fn test_hook_skips_non_lex_kind() {
        let mut hook = FulltextHook::new();

        // Fulltext dt_id but wrong o_kind (e.g., REF_ID)
        hook.on_op(0, 5, DatatypeDictId::FULL_TEXT.as_u16(), ObjKind::REF_ID.as_u8(), 42, 1, true);
        assert!(hook.is_empty());

        // Fulltext dt_id but NUM_INT o_kind
        hook.on_op(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            ObjKind::NUM_INT.as_u8(),
            42,
            1,
            true,
        );
        assert!(hook.is_empty());
    }

    #[test]
    fn test_hook_tracks_retractions() {
        let mut hook = FulltextHook::new();
        hook.on_op(0, 5, DatatypeDictId::FULL_TEXT.as_u16(), LEX, 42, 1, true);
        hook.on_op(0, 5, DatatypeDictId::FULL_TEXT.as_u16(), LEX, 42, 2, false);
        assert_eq!(hook.entry_count(), 2);
        assert!(hook.entries()[0].is_assert);
        assert!(!hook.entries()[1].is_assert);
    }

    #[test]
    fn test_into_entries() {
        let mut hook = FulltextHook::new();
        hook.on_op(0, 5, DatatypeDictId::FULL_TEXT.as_u16(), LEX, 10, 1, true);
        hook.on_op(1, 7, DatatypeDictId::FULL_TEXT.as_u16(), LEX, 20, 2, true);
        let entries = hook.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].g_id, 0);
        assert_eq!(entries[1].g_id, 1);
    }
}
