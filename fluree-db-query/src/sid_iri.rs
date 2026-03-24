use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{LedgerSnapshot, Sid};
use std::borrow::Cow;
use std::io;

/// Decode a `Sid` into a full IRI string, using the snapshot namespace table.
///
/// This is the canonical "query-space Sid → canonical IRI" operation:
/// - Prefer `LedgerSnapshot::decode_sid` (authoritative for query/overlay Sids).
/// - If the Sid is in the EMPTY namespace (code 0), treat `sid.name` as the full IRI.
/// - Otherwise return `None` (cannot decode).
#[inline]
pub(crate) fn sid_to_full_iri<'a>(
    snapshot: &'a LedgerSnapshot,
    sid: &'a Sid,
) -> Option<Cow<'a, str>> {
    snapshot.decode_sid(sid).map(Cow::Owned).or_else(|| {
        (sid.namespace_code == fluree_vocab::namespaces::EMPTY)
            .then_some(Cow::Borrowed(sid.name.as_ref()))
    })
}

/// Translate a query-space `Sid` (snapshot namespace codes) into a store-space `Sid`.
///
/// Returns `None` if the `Sid` cannot be decoded to a full IRI via the snapshot.
#[inline]
pub(crate) fn sid_to_store_sid(
    snapshot: &LedgerSnapshot,
    store: &BinaryIndexStore,
    sid: &Sid,
) -> Option<Sid> {
    let iri = sid_to_full_iri(snapshot, sid)?;
    Some(store.encode_iri(iri.as_ref()))
}

/// Translate a query-space `Sid` into a persisted subject ID filter (`s_id`) for the binary store.
///
/// Returns `Ok(None)` when the Sid cannot be decoded (unknown namespace) or when the
/// subject is not present in the persisted dictionaries (common for novelty-only).
#[inline]
pub(crate) fn sid_to_store_s_id(
    snapshot: &LedgerSnapshot,
    store: &BinaryIndexStore,
    sid: &Sid,
) -> io::Result<Option<u64>> {
    let Some(store_sid) = sid_to_store_sid(snapshot, store, sid) else {
        return Ok(None);
    };
    store.sid_to_s_id(&store_sid)
}

/// Translate a query-space `Sid` into a persisted predicate ID filter (`p_id`) for the binary store.
///
/// Returns `None` when the Sid cannot be decoded (unknown namespace) or when the
/// predicate is not present in the persisted predicate dictionary (novelty-only predicate).
#[inline]
pub(crate) fn sid_to_store_p_id(
    snapshot: &LedgerSnapshot,
    store: &BinaryIndexStore,
    sid: &Sid,
) -> Option<u32> {
    let store_sid = sid_to_store_sid(snapshot, store, sid)?;
    store.sid_to_p_id(&store_sid)
}
