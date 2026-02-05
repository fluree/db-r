# fluree query

Query a ledger.

## Usage

```bash
fluree query [LEDGER] [FILE] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger + stdin or `-e` expression |
| `<arg>` | If file exists: active ledger + file; else: ledger alias + stdin/-e |
| `<ledger> <file>` | Specified ledger + file |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <EXPR>` | Inline query expression |
| `--format <FORMAT>` | Output format: `json`, `table`, or `csv` (default: `json`) |
| `--sparql` | Force SPARQL query format |
| `--fql` | Force FQL (JSON-LD) query format |
| `--at <TIME>` | Query at a specific point in time |

## Description

Executes a query against a ledger. Supports both SPARQL and FQL (Fluree Query Language / JSON-LD queries).

## Query Formats

### SPARQL

```bash
fluree query --sparql -e 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```

### FQL (JSON-LD Query)

```bash
fluree query --fql -e '{"select": ["?name"], "where": {"http://example.org/name": "?name"}}'
```

Format is auto-detected if not specified:
- Contains `SELECT`, `CONSTRUCT`, `ASK`, or `DESCRIBE` → SPARQL
- Otherwise → FQL

## Output Formats

### JSON (default)

```bash
fluree query --sparql -e 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```json
{
  "head": {"vars": ["name"]},
  "results": {"bindings": [{"name": {"type": "literal", "value": "Alice"}}]}
}
```

### Table

```bash
fluree query --sparql --format table -e 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
┌───────┐
│ name  │
├───────┤
│ Alice │
│ Bob   │
└───────┘
```

### CSV

```bash
fluree query --sparql --format csv -e 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```
```
name
Alice
Bob
```

## Time Travel

Query historical states with `--at`:

```bash
# Query at transaction 5
fluree query --at 5 --sparql -e 'SELECT * WHERE { ?s ?p ?o }'

# Query at specific commit
fluree query --at abc123def --sparql -e 'SELECT * WHERE { ?s ?p ?o }'

# Query at ISO-8601 timestamp
fluree query --at 2024-01-15T10:30:00Z --sparql -e 'SELECT * WHERE { ?s ?p ?o }'
```

## Examples

```bash
# SPARQL query from file
fluree query --sparql query.rq

# FQL query inline
fluree query -e '{"select": {"?s": ["*"]}, "where": {"@id": "?s"}}'

# Query specific ledger with CSV output
fluree query production --sparql --format csv -e 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10'

# Time travel query
fluree query --at 3 --sparql -e 'SELECT * WHERE { ?s ?p ?o }'
```

## See Also

- [history](history.md) - View entity change history
- [export](export.md) - Export all data
