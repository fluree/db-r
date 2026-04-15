# TTL file format

The `.fluree-memory/repo.ttl` and `.fluree-memory/.local/user.ttl` files hold the serialized form of every memory in their respective scope. Each memory is a block of [Turtle](https://www.w3.org/TR/turtle/) triples.

## Structure

Each memory is a Turtle subject block: the IRI, followed by `a mem:<Kind>` (RDF type), then a predicate list in a canonical order. Multi-valued predicates (`mem:tag`, `mem:artifactRef`) repeat once per value.

```ttl
# Fluree Memory — repo-scoped
# Auto-managed by `fluree memory`. Manual edits are supported.
@prefix mem: <https://ns.flur.ee/memory#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

mem:fact-01JDXYZ a mem:Fact ;
    mem:content "Tests use cargo nextest" ;
    mem:tag "cargo" ;
    mem:tag "testing" ;
    mem:scope mem:repo ;
    mem:createdAt "2026-02-22T14:00:00Z"^^xsd:dateTime .

mem:decision-01JDABC a mem:Decision ;
    mem:content "Use postcard for compact index encoding" ;
    mem:tag "encoding" ;
    mem:tag "indexer" ;
    mem:scope mem:repo ;
    mem:artifactRef "fluree-db-indexer/" ;
    mem:createdAt "2026-02-22T14:05:00Z"^^xsd:dateTime ;
    mem:rationale "no_std compatible, smaller output than bincode" ;
    mem:alternatives "bincode, CBOR, MessagePack" .
```

Tags and artifact refs are sorted alphabetically within a memory for deterministic diffs. When a memory is updated, the TTL file is rewritten with the changes in place and git tracks the history.

## Why TTL and not JSON

Three reasons:

- **Diff-friendly** — predicates are one per line within a subject block, so git diffs are readable and appended memories tend to merge cleanly.
- **Streamable** — the writer doesn't need to serialize the whole file; appends are cheap.
- **Semantically exact** — Turtle is RDF, so there's no impedance mismatch between what's in the file and what's in the `__memory` ledger.

## Sync direction

The TTL file is the **canonical** store for a given scope. The `__memory` ledger is a derived cache rebuilt from the TTL files when they change.

When you `memory add`, the CLI / MCP server:

1. Appends the new memory block to the appropriate TTL file (authoritative).
2. Transacts the new triples into the `__memory` ledger (so recall is fast).
3. Writes a content-hash watermark to `.fluree-memory/.local/build-hash`.

If the ledger write fails, the hash is left stale and the next `ensure_synced` call rebuilds the ledger from the files. When git pulls in a new version of `repo.ttl`, the hash mismatch triggers the same rebuild. In practice this is invisible.

## Editing by hand

You *can* edit `repo.ttl` or `user.ttl` directly if you need to — fix a typo, reorder, batch-retag. After editing:

```bash
fluree memory status
```

…to verify the store parses cleanly. If there's a syntax error, `status` will point at it.

For most fixes, though, prefer `update` / `forget` — they'll produce cleaner git history than hand-edits.

## File size

TTL is compact. A project with ~200 memories typically lands under 50 KB. At that size, `repo.ttl` stays pleasant to review in a PR.

If a file grows past that, consider whether you're memorizing task state instead of durable knowledge — a `fluree memory status` + skim + cleanup pass is usually all it takes.
