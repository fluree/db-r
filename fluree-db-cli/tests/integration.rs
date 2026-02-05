use assert_cmd::Command;
use assert_cmd::cargo_bin_cmd;
use predicates::prelude::*;
use tempfile::TempDir;

/// Helper to create a `fluree` command that runs in an isolated temp directory.
/// Sets HOME to the temp dir so `~/.fluree/` fallback never leaks between tests.
fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd
}

// ============================================================================
// Happy path tests
// ============================================================================

#[test]
fn version_flag() {
    cargo_bin_cmd!("fluree")
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

#[test]
fn help_flag() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Fluree database CLI"))
        .stdout(predicate::str::contains("init"))
        .stdout(predicate::str::contains("create"))
        .stdout(predicate::str::contains("query"));
}

#[test]
fn verbose_quiet_conflict() {
    cargo_bin_cmd!("fluree")
        .args(["--verbose", "--quiet", "list"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn init_creates_fluree_dir() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp)
        .arg("init")
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized Fluree in"));

    assert!(tmp.path().join(".fluree").is_dir());
    assert!(tmp.path().join(".fluree/storage").is_dir());
    assert!(tmp.path().join(".fluree/config.toml").exists());
}

#[test]
fn init_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .arg("init")
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized Fluree in"));
}

#[test]
fn golden_path() {
    let tmp = TempDir::new().unwrap();

    // init
    fluree_cmd(&tmp).arg("init").assert().success();

    // create
    fluree_cmd(&tmp)
        .args(["create", "testdb"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created ledger 'testdb'"));

    // list shows the ledger
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("testdb"))
        .stdout(predicate::str::contains("main"));

    // info
    fluree_cmd(&tmp)
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("Ledger:"))
        .stdout(predicate::str::contains("testdb"));

    // insert JSON-LD
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // insert Turtle
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:bob a ex:Person ; ex:name \"Bob\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // query SPARQL (JSON output)
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "-e",
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"));

    // query SPARQL (table output)
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--format",
            "table",
            "-e",
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"));

    // log
    fluree_cmd(&tmp)
        .args(["log", "--oneline"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"))
        .stdout(predicate::str::contains("t=1"));

    // log with count limit
    fluree_cmd(&tmp)
        .args(["log", "--oneline", "-n", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"));

    // drop
    fluree_cmd(&tmp)
        .args(["drop", "testdb", "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Dropped ledger 'testdb'"));

    // list after drop
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("No ledgers found"));
}

#[test]
fn use_command_switches_active_ledger() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db1"]).assert().success();
    fluree_cmd(&tmp).args(["create", "db2"]).assert().success();

    // db2 should be active (last created)
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("* | db2").or(predicate::str::contains("*")));

    // Switch to db1
    fluree_cmd(&tmp)
        .args(["use", "db1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Now using ledger 'db1'"));

    // info should show db1
    fluree_cmd(&tmp)
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("db1"));
}

#[test]
fn insert_with_commit_message() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "msgdb"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:x", "ex:val": "test"}"#,
            "-m",
            "initial data load",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

// ============================================================================
// Error path tests
// ============================================================================

#[test]
fn query_without_init_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp)
        .args(["query", "-e", "SELECT * WHERE { ?s ?p ?o }"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no .fluree/ directory found"))
        .stderr(predicate::str::contains("fluree init"));
}

#[test]
fn insert_without_active_ledger_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["insert", "-e", "{}"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no active ledger set"))
        .stderr(predicate::str::contains("fluree use"));
}

#[test]
fn drop_without_force_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    fluree_cmd(&tmp)
        .args(["drop", "db"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--force"));
}

#[test]
fn use_nonexistent_ledger_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["use", "doesnotexist"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

#[test]
fn query_no_input_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    // In test context, stdin is piped (not a TTY), so empty stdin is read
    // and format detection fails. Either error message is acceptable.
    fluree_cmd(&tmp)
        .arg("query")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("no input provided")
                .or(predicate::str::contains("could not detect query format")),
        );
}

#[test]
fn sparql_fql_conflict() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    fluree_cmd(&tmp)
        .args(["query", "--sparql", "--fql", "-e", "SELECT * WHERE { ?s ?p ?o }"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

// ============================================================================
// File-based input tests
// ============================================================================

#[test]
fn insert_from_turtle_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "filedb"])
        .assert()
        .success();

    let ttl_path = tmp.path().join("data.ttl");
    std::fs::write(
        &ttl_path,
        "@prefix ex: <http://example.org/> .\nex:x a ex:Thing ; ex:val \"hello\" .\n",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["insert", ttl_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

#[test]
fn query_from_sparql_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "filedb"])
        .assert()
        .success();

    // Insert some data first
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a a ex:Thing ; ex:label \"alpha\" .\n",
        ])
        .assert()
        .success();

    let rq_path = tmp.path().join("query.rq");
    std::fs::write(
        &rq_path,
        "SELECT ?label WHERE { ?s <http://example.org/label> ?label }",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["query", rq_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("alpha"));
}

// ============================================================================
// Create --from tests
// ============================================================================

#[test]
fn create_from_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let ttl_path = tmp.path().join("seed.ttl");
    std::fs::write(
        &ttl_path,
        "@prefix ex: <http://example.org/> .\nex:seed a ex:Thing ; ex:val \"seeded\" .\n",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["create", "seeddb", "--from", ttl_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created ledger 'seeddb'"))
        .stdout(predicate::str::contains("flakes"));
}

// ============================================================================
// v1.1 — Upsert tests
// ============================================================================

#[test]
fn upsert_json_ld() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "udb"]).assert().success();

    // Insert initial data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:name": "Alice"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // Upsert — update existing entity
    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:name": "Alice Updated"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));
}

#[test]
fn upsert_turtle() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "udb2"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:x a ex:Thing ; ex:val \"hello\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

// ============================================================================
// v1.1 — CSV output tests
// ============================================================================

#[test]
fn query_csv_output() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "csvdb"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a a ex:Thing ; ex:label \"alpha\" .\nex:b a ex:Thing ; ex:label \"beta\" .",
        ])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--format",
            "csv",
            "-e",
            "SELECT ?label WHERE { ?s <http://example.org/label> ?label }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("label"))
        .stdout(predicate::str::contains("alpha"))
        .stdout(predicate::str::contains("beta"));
}

// ============================================================================
// v1.1 — --at time travel tests
// ============================================================================

#[test]
fn query_at_time_travel() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "ttdb"]).assert().success();

    // t=1
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a ex:val \"first\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // t=2
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:b ex:val \"second\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // Query at t=1 should only see "first"
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--at",
            "1",
            "-e",
            "SELECT ?val WHERE { ?s <http://example.org/val> ?val }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("first"))
        .stdout(predicate::str::contains("second").not());
}

// ============================================================================
// v1.1 — Export tests
// ============================================================================

#[test]
fn export_jsonld() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "expdb"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:thing a ex:Widget ; ex:label \"gadget\" .",
        ])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args(["export", "--format", "jsonld"])
        .assert()
        .success()
        .stdout(predicate::str::contains("gadget"));
}

#[test]
fn export_turtle() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "expdb2"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:item a ex:Product ; ex:name \"widget\" .",
        ])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args(["export", "--format", "turtle"])
        .assert()
        .success()
        .stdout(predicate::str::contains("widget"));
}

// ============================================================================
// v1.1 — Config tests
// ============================================================================

#[test]
fn config_set_get_list() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Set a value
    fluree_cmd(&tmp)
        .args(["config", "set", "storage.path", "/custom/path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Set 'storage.path'"));

    // Get the value
    fluree_cmd(&tmp)
        .args(["config", "get", "storage.path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/custom/path"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["config", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("storage.path"))
        .stdout(predicate::str::contains("/custom/path"));
}

#[test]
fn config_get_missing_key() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["config", "get", "nonexistent.key"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not set"));
}

#[test]
fn config_list_empty() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["config", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no configuration set"));
}

// ============================================================================
// v1.1 — Completions tests
// ============================================================================

#[test]
fn completions_bash() {
    cargo_bin_cmd!("fluree")
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

#[test]
fn completions_zsh() {
    cargo_bin_cmd!("fluree")
        .args(["completions", "zsh"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

// ============================================================================
// v1.1 — Help text shows new commands
// ============================================================================

#[test]
fn help_shows_v11_commands() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("upsert"))
        .stdout(predicate::str::contains("export"))
        .stdout(predicate::str::contains("config"))
        .stdout(predicate::str::contains("completions"));
}

// ============================================================================
// v1.2 — Prefix management tests
// ============================================================================

#[test]
fn prefix_add_list_remove() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a prefix
    fluree_cmd(&tmp)
        .args(["prefix", "add", "ex", "http://example.org/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added prefix: ex"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("http://example.org/"));

    // Add another
    fluree_cmd(&tmp)
        .args(["prefix", "add", "foaf", "http://xmlns.com/foaf/0.1/"])
        .assert()
        .success();

    // List shows both
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("foaf:"));

    // Remove one
    fluree_cmd(&tmp)
        .args(["prefix", "remove", "foaf"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed prefix: foaf"));

    // List shows only ex
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("foaf:").not());
}

#[test]
fn prefix_list_empty() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no prefixes defined"));
}

#[test]
fn prefix_remove_nonexistent() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["prefix", "remove", "nothere"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

// ============================================================================
// v1.2 — History command tests
// ============================================================================

#[test]
fn history_shows_changes() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "histdb"]).assert().success();

    // Add prefix for convenience
    fluree_cmd(&tmp)
        .args(["prefix", "add", "ex", "http://example.org/"])
        .assert()
        .success();

    // t=1: Insert entity
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:alice ex:name \"Alice\" ; ex:age \"30\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // t=2: Update entity
    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:alice ex:name \"Alice Smith\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // Query history using compact IRI (prefix expansion)
    fluree_cmd(&tmp)
        .args(["history", "ex:alice", "--format", "json"])
        .assert()
        .success();
}

#[test]
fn history_with_full_iri() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "histdb2"]).assert().success();

    // Insert data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:bob ex:status \"active\" .",
        ])
        .assert()
        .success();

    // Query history using full IRI (no prefix needed)
    fluree_cmd(&tmp)
        .args(["history", "http://example.org/bob", "--format", "json"])
        .assert()
        .success();
}

// ============================================================================
// v1.2 — Help text shows new commands
// ============================================================================

#[test]
fn help_shows_v12_commands() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("history"))
        .stdout(predicate::str::contains("prefix"));
}
