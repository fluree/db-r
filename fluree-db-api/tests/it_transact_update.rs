//! Transact update integration tests (Clojure parity)
//!
//! Ports the `deleting-data` section from:
//! `db-clojure/test/fluree/db/transact/update_test.clj`
//!
//! Note: The `transaction-functions` section (hash/datetime) is covered with bind support.

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::Db;
use serde_json::{json, Value as JsonValue};

fn ctx_ex_schema() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    })
}

fn ctx_ex() -> JsonValue {
    json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.com/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_users(
    ledger_id: &str,
) -> (
    fluree_db_api::Fluree<
        fluree_db_core::MemoryStorage,
        fluree_db_nameservice::memory::MemoryNameService,
    >,
    LedgerState,
) {
    let fluree = FlureeBuilder::memory().build_memory();

    let db0 = Db::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex_schema(),
                "insert": {
                    "@graph": [
                        {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@flur.ee","schema:age":42},
                        {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22},
                        {"@id":"ex:jane","@type":"ex:User","schema:name":"Jane","schema:email":"jane@flur.ee","schema:age":30}
                    ]
                }
            }),
        )
        .await
        .expect("seed update insert");

    (fluree, seeded.ledger)
}

async fn query_names(
    fluree: &fluree_db_api::Fluree<
        fluree_db_core::MemoryStorage,
        fluree_db_nameservice::memory::MemoryNameService,
    >,
    ledger: &LedgerState,
) -> Vec<String> {
    let q = json!({
        "@context": ctx_ex_schema(),
        "select": ["?name"],
        "where": {"schema:name": "?name"}
    });
    let result = fluree.query(ledger, &q).await.expect("query names");
    let v = result.to_jsonld(&ledger.db).expect("to_jsonld");
    let mut out: Vec<String> = v
        .as_array()
        .expect("array")
        .iter()
        .map(|x| x.as_str().expect("name string").to_string())
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn update_delete_subject_ex_alice_removes_only_alice() {
    let (fluree, db) = seed_users("it/transact-update:delete-subject").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "where":  { "@id": "ex:alice", "?p": "?o" },
                "delete": { "@id": "ex:alice", "?p": "?o" }
            }),
        )
        .await
        .expect("delete subject");

    assert_eq!(query_names(&fluree, &out.ledger).await, vec!["Bob", "Jane"]);
}

#[tokio::test]
async fn update_delete_bob_age_only() {
    let (fluree, db) = seed_users("it/transact-update:delete-bob-age").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "ex:bob", "schema:age": "?o" },
                "where":  { "@id": "ex:bob", "schema:age": "?o" }
            }),
        )
        .await
        .expect("delete bob age");

    let q_bob = json!({ "@context": ctx_ex_schema(), "selectOne": { "ex:bob": ["*"] }});
    let bob = fluree
        .query(&out.ledger, &q_bob)
        .await
        .expect("query bob")
        .to_jsonld_async(&out.ledger.db)
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        bob,
        json!({"@id":"ex:bob","@type":"ex:User","schema:name":"Bob"})
    );
}

#[tokio::test]
async fn update_delete_all_subjects_with_email_predicate() {
    let (fluree, db) = seed_users("it/transact-update:delete-has-email").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "?s", "?p": "?o" },
                "where":  { "@id": "?s", "schema:email": "?x", "?p": "?o" }
            }),
        )
        .await
        .expect("delete all with email");

    assert_eq!(query_names(&fluree, &out.ledger).await, vec!["Bob"]);
}

#[tokio::test]
async fn update_delete_all_subjects_where_age_equals_30() {
    let (fluree, db) = seed_users("it/transact-update:delete-age-30").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "?s", "?p": "?o" },
                "where":  { "@id": "?s", "schema:age": 30, "?p": "?o" }
            }),
        )
        .await
        .expect("delete by age=30");

    assert_eq!(
        query_names(&fluree, &out.ledger).await,
        vec!["Alice", "Bob"]
    );
}

#[tokio::test]
async fn update_bob_age_when_match() {
    let (fluree, db) = seed_users("it/transact-update:update-bob-when-match").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "ex:bob", "schema:age": 22 },
                "insert": { "@id": "ex:bob", "schema:age": 23 },
                "where":  { "@id": "ex:bob", "schema:age": 22 }
            }),
        )
        .await
        .expect("update bob age when match");

    let bob = fluree
        .query(
            &out.ledger,
            &json!({"@context": ctx_ex_schema(), "selectOne": {"ex:bob": ["*"]}}),
        )
        .await
        .expect("query bob")
        .to_jsonld_async(&out.ledger.db)
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        bob,
        json!({"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":23})
    );
}

#[tokio::test]
async fn update_no_match_is_noop_success_and_does_not_bump_t() {
    let (fluree, db) = seed_users("it/transact-update:no-match-noop").await;
    let t_before = db.t();

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "?s", "schema:age": 99 },
                "insert": { "@id": "?s", "schema:age": 23 },
                "where":  { "@id": "?s", "schema:age": 99 }
            }),
        )
        .await
        .expect("no-op update should succeed");

    assert_eq!(out.ledger.t(), t_before);

    let bob = fluree
        .query(
            &out.ledger,
            &json!({"@context": ctx_ex_schema(), "selectOne": {"ex:bob": ["*"]}}),
        )
        .await
        .expect("query bob")
        .to_jsonld_async(&out.ledger.db)
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        bob,
        json!({"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22})
    );
}

#[tokio::test]
async fn update_replace_jane_age() {
    let (fluree, db) = seed_users("it/transact-update:update-jane").await;

    let out = fluree
        .update(
            db,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "ex:jane", "schema:age": "?current_age" },
                "insert": { "@id": "ex:jane", "schema:age": 31 },
                "where":  { "@id": "ex:jane", "schema:age": "?current_age" }
            }),
        )
        .await
        .expect("update jane age");

    let jane = fluree
        .query(
            &out.ledger,
            &json!({"@context": ctx_ex_schema(), "selectOne": {"ex:jane": ["*"]}}),
        )
        .await
        .expect("query jane")
        .to_jsonld_async(&out.ledger.db)
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        jane,
        json!({"@id":"ex:jane","@type":"ex:User","schema:name":"Jane","schema:email":"jane@flur.ee","schema:age":31})
    );
}

#[tokio::test]
async fn update_where_bind_hash_functions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:hash-functions");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:md5":0,"ex:sha1":0,"ex:sha256":0,"ex:sha384":0,"ex:sha512":0},
                        {"id":"ex:hash-fns","ex:message":"abc"}
                    ]
                }
            }),
        )
        .await
        .expect("seed hash fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "delete": [],
                "where": [
                    {"id":"ex:hash-fns","ex:message":"?message"},
                    ["bind", "?sha256", "(sha256 ?message)", "?sha512", "(sha512 ?message)"]
                ],
                "insert": {"id":"ex:hash-fns","ex:sha256":"?sha256","ex:sha512":"?sha512"}
            }),
        )
        .await
        .expect("update hash fns");

    let result = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:hash-fns": ["ex:sha512","ex:sha256"]}
            }),
        )
        .await
        .expect("query hash fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        result,
        json!({
            "ex:sha512": "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
            "ex:sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        })
    );
}

#[tokio::test]
async fn update_where_bind_datetime_functions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:datetime-functions");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:now":0,"ex:year":0,"ex:month":0,"ex:day":0,"ex:hours":0,"ex:minutes":0,"ex:seconds":0,"ex:timezone":0,"ex:tz":0},
                        {"id":"ex:datetime-fns",
                         "ex:localdatetime":{"@value":"2023-06-13T14:17:22.435","@type":"xsd:dateTime"},
                         "ex:offsetdatetime":{"@value":"2023-06-13T14:17:22.435-05:00","@type":"xsd:dateTime"},
                         "ex:utcdatetime":{"@value":"2023-06-13T14:17:22.435Z","@type":"xsd:dateTime"}}
                    ]
                }
            }),
        )
        .await
        .expect("seed datetime fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:datetime-fns","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:localdatetime":"?localdatetime","ex:offsetdatetime":"?offsetdatetime","ex:utcdatetime":"?utcdatetime"},
                    ["bind",
                     "?now", "(now)",
                     "?year", "(year ?localdatetime)",
                     "?month", "(month ?localdatetime)",
                     "?day", "(day ?localdatetime)",
                     "?hours", "(hours ?localdatetime)",
                     "?minutes", "(minutes ?localdatetime)",
                     "?seconds", "(seconds ?localdatetime)",
                     "?tz1", "(tz ?utcdatetime)",
                     "?tz2", "(tz ?offsetdatetime)",
                     "?comp=", "(= ?localdatetime (now))",
                     "?comp<", "(< ?localdatetime (now))",
                     "?comp<=", "(<= ?localdatetime (now))",
                     "?comp>", "(> ?localdatetime (now))",
                     "?comp>=", "(>= ?localdatetime (now))"]
                ],
                "insert": {"id":"?s",
                           "ex:now":"?now",
                           "ex:year":"?year",
                           "ex:month":"?month",
                           "ex:day":"?day",
                           "ex:hours":"?hours",
                           "ex:minutes":"?minutes",
                           "ex:seconds":"?seconds",
                           "ex:tz":["?tz1","?tz2"],
                           "ex:comp=":"?comp=",
                           "ex:comp<":"?comp<",
                           "ex:comp<=":"?comp<=",
                           "ex:comp>":"?comp>",
                           "ex:comp>=":"?comp>="}
            }),
        )
        .await
        .expect("update datetime fns");

    let result = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:datetime-fns": ["ex:now","ex:year","ex:month","ex:day","ex:hours","ex:minutes","ex:seconds","ex:tz","ex:comp=","ex:comp<","ex:comp<=","ex:comp>","ex:comp>="]}
            }),
        )
        .await
        .expect("query datetime fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    let now_val = result.get("ex:now").expect("now");
    assert!(now_val.is_string() || now_val.get("@value").is_some());
    assert_eq!(result.get("ex:year").and_then(|v| v.as_i64()), Some(2023));
    assert_eq!(result.get("ex:month").and_then(|v| v.as_i64()), Some(6));
    assert_eq!(result.get("ex:day").and_then(|v| v.as_i64()), Some(13));
    assert_eq!(result.get("ex:hours").and_then(|v| v.as_i64()), Some(14));
    assert_eq!(result.get("ex:minutes").and_then(|v| v.as_i64()), Some(17));
    assert_eq!(result.get("ex:seconds").and_then(|v| v.as_i64()), Some(22));
    let mut tz_values: Vec<&str> = result
        .get("ex:tz")
        .and_then(|v| v.as_array())
        .expect("tz array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    tz_values.sort();
    assert_eq!(tz_values, vec!["+00:00", "-05:00"]);
    assert_eq!(result.get("ex:comp="), Some(&json!(false)));
    assert_eq!(result.get("ex:comp<"), Some(&json!(true)));
    assert_eq!(result.get("ex:comp<="), Some(&json!(true)));
    assert_eq!(result.get("ex:comp>"), Some(&json!(false)));
    assert_eq!(result.get("ex:comp>="), Some(&json!(false)));
}

#[tokio::test]
async fn update_where_bind_numeric_and_math_functions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:numeric-functions");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:abs":0,"ex:round":0,"ex:ceil":0,"ex:floor":0,"ex:rand":0,"ex:result":0},
                        {"id":"ex:numeric-fns","ex:pos-int":2,"ex:neg-int":-2,"ex:decimal":1.4},
                        {"id":"ex:math","ex:num":0}
                    ]
                }
            }),
        )
        .await
        .expect("seed numeric fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:numeric-fns","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:pos-int":"?pos-int","ex:neg-int":"?neg-int","ex:decimal":"?decimal"},
                    ["bind",
                     "?abs", "(abs ?neg-int)",
                     "?round", "(round ?decimal)",
                     "?ceil", "(ceil ?decimal)",
                     "?floor", "(floor ?decimal)",
                     "?rand", "(rand)"]
                ],
                "insert": {"id":"?s",
                           "ex:abs":"?abs",
                           "ex:round":"?round",
                           "ex:ceil":"?ceil",
                           "ex:floor":"?floor",
                           "ex:rand":"?rand"}
            }),
        )
        .await
        .expect("update numeric fns");

    let updated = fluree
        .update(
            updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:math","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:num":"?num"},
                    ["bind", "?result", "(* (* (* (- (/ (+ ?num 10) 2) 3) (- (/ (+ ?num 10) 2) 3)) (- (/ (+ ?num 10) 2) 3)) 10)"]
                ],
                "insert": {"id":"?s","ex:result":"?result"}
            }),
        )
        .await
        .expect("update math fns");

    let numeric = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:numeric-fns": ["ex:abs","ex:round","ex:ceil","ex:floor","ex:rand"]}
            }),
        )
        .await
        .expect("query numeric fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    let num_val = |v: &serde_json::Value| v.as_i64().map(|n| n as f64).or_else(|| v.as_f64());
    assert_eq!(numeric.get("ex:abs").and_then(num_val), Some(2.0));
    assert_eq!(numeric.get("ex:round").and_then(num_val), Some(1.0));
    assert_eq!(numeric.get("ex:ceil").and_then(num_val), Some(2.0));
    assert_eq!(numeric.get("ex:floor").and_then(num_val), Some(1.0));
    assert!(numeric.get("ex:rand").is_some());

    let math = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:math": ["ex:result"]}
            }),
        )
        .await
        .expect("query math fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    assert_eq!(math.get("ex:result").and_then(num_val), Some(80.0));
}

#[tokio::test]
async fn update_where_bind_string_functions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:string-functions");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:strLen":0,"ex:subStr":0,"ex:ucase":0,"ex:lcase":0,"ex:strStarts":0,"ex:strEnds":0,"ex:contains":0,"ex:strBefore":0,"ex:strAfter":0,"ex:concat":0,"ex:regex":0},
                        {"id":"ex:string-fns","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed string fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:string-fns","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind",
                     "?strlen", "(strLen ?text)",
                     "?sub1", "(subStr ?text 5)",
                     "?sub2", "(subStr ?text 1 4)",
                     "?upcased", "(ucase ?text)",
                     "?downcased", "(lcase ?text)",
                     "?a-start", "(strStarts ?text \"x\")",
                     "?a-end", "(strEnds ?text \"x\")",
                     "?contains", "(contains ?text \"x\")",
                     "?strBefore", "(strBefore ?text \"bcd\")",
                     "?strAfter", "(strAfter ?text \"bcd\")",
                     "?concatted", ["concat", "?text", " ", "STR1 ", "STR2"],
                     "?matched", "(regex ?text \"^Abc\")"]
                ],
                "insert": {"id":"?s",
                           "ex:strStarts":"?a-start",
                           "ex:strEnds":"?a-end",
                           "ex:subStr":["?sub1","?sub2"],
                           "ex:strLen":"?strlen",
                           "ex:ucase":"?upcased",
                           "ex:lcase":"?downcased",
                           "ex:contains":"?contains",
                           "ex:strBefore":"?strBefore",
                           "ex:strAfter":"?strAfter",
                           "ex:concat":"?concatted",
                           "ex:regex":"?matched"}
            }),
        )
        .await
        .expect("update string fns");

    let result = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:string-fns": ["ex:strLen","ex:subStr","ex:ucase","ex:lcase","ex:strStarts","ex:strEnds","ex:contains","ex:strBefore","ex:strAfter","ex:concat","ex:regex"]}
            }),
        )
        .await
        .expect("query string fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        result,
        json!({
            "ex:strEnds": false,
            "ex:strStarts": false,
            "ex:contains": false,
            "ex:regex": true,
            "ex:subStr": ["Abcd", "efg"],
            "ex:strLen": 7,
            "ex:ucase": "ABCDEFG",
            "ex:lcase": "abcdefg",
            "ex:strBefore": "A",
            "ex:strAfter": "efg",
            "ex:concat": "Abcdefg STR1 STR2"
        })
    );
}

#[tokio::test]
async fn update_where_bind_functional_forms() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:functional-forms");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:bound":0,"ex:if":0,"ex:coalesce":0,"ex:logical-or":0,"ex:logical-and":0},
                        {"id":"ex:functional-fns","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed functional fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:functional-fns","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind",
                     "?bound", "(bound ?text)",
                     "?if", "(if true 1 2)",
                     "?coalesce", "(coalesce ?missing ?text)",
                     // Keep IN list literals simple (no nested expressions) to avoid
                     // depending on unsupported parsing of expression forms inside vectors.
                     "?in", "(in (strLen ?text) [7 8 9])",
                     "?not-in", "(not (in (strLen ?text) [7 8 9]))",
                     "?or", "(or false false false (= 0 (- (- 10 3) 7)))",
                     // Ensure boolean output (avoid truthy non-bool semantics).
                     "?and", "(and true true true (= (+ (- 10 3) 7) 14))"]
                ],
                "insert": {"id":"?s",
                           "ex:bound":"?bound",
                           "ex:if":"?if",
                           "ex:coalesce":"?coalesce",
                           "ex:in":"?in",
                           "ex:not-in":"?not-in",
                           "ex:logical-or":"?or",
                           "ex:logical-and":"?and"}
            }),
        )
        .await
        .expect("update functional fns");

    let result = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:functional-fns": ["ex:bound","ex:if","ex:coalesce","ex:in","ex:not-in","ex:logical-or","ex:logical-and"]}
            }),
        )
        .await
        .expect("query functional fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        result,
        json!({
            "ex:bound": true,
            "ex:if": 1,
            "ex:coalesce": "Abcdefg",
            "ex:in": true,
            "ex:not-in": false,
            "ex:logical-or": true,
            "ex:logical-and": true
        })
    );
}

#[tokio::test]
async fn update_where_bind_rdf_term_functions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:rdf-term-functions");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:isBlank":0,"ex:isNumeric":0,"ex:str":0,"ex:IRI":0,"ex:isIRI":0,"ex:isLiteral":0,"ex:strdt":0,"ex:strLang":0,"ex:bnode":0,"ex:lang":0,"ex:datatype":0},
                        {"id":"ex:rdf-term-fns",
                         "ex:text":"Abcdefg",
                         "ex:langText":{"@value":"hola","@language":"es"},
                         "ex:number":1}
                    ]
                }
            }),
        )
        .await
        .expect("seed rdf term fns");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "values": ["?s", [{"@value":"ex:rdf-term-fns","@type":"@id"}]],
                "where": [
                    {"id":"?s","ex:text":"?text","ex:langText":"?langtext","ex:number":"?num"},
                    ["bind",
                     "?str", "(str ?num)",
                     "?str2", "(str ?text)",
                     "?lang", "(lang ?langtext)",
                     "?datatype", "(datatype ?langtext)",
                     "?IRI", "(iri (concat \"http://example.com/\" ?text))",
                     "?isIRI", "(is-iri ?IRI)",
                     "?isLiteral", "(is-literal ?num)",
                     "?strdt", "(str-dt ?text \"http://example.com/mystring\")",
                     "?strLang", "(str-lang ?text \"foo\")",
                     "?bnode", "(bnode)"]
                ],
                "insert": {"id":"?s",
                           "ex:str":["?str","?str2"],
                           "ex:isNumeric":"?isLiteral",
                           "ex:lang":"?lang",
                           "ex:datatype":"?datatype",
                           "ex:IRI":"?IRI",
                           "ex:isIRI":"?isIRI",
                           "ex:isLiteral":"?isLiteral",
                           "ex:strdt":"?strdt",
                           "ex:strLang":"?strLang",
                           "ex:bnode":"?bnode"}
            }),
        )
        .await
        .expect("update rdf term fns");

    let result = fluree
        .query(
            &updated.ledger,
            &json!({
                "@context": ctx_ex(),
                "selectOne": {"ex:rdf-term-fns": ["ex:isIRI","ex:isLiteral","ex:lang","ex:datatype","ex:IRI","ex:bnode","ex:strdt","ex:strLang","ex:str"]}
            }),
        )
        .await
        .expect("query rdf term fns")
        .to_jsonld_async(&updated.ledger.db)
        .await
        .expect("to_jsonld_async");

    let bnode = result.get("ex:bnode").expect("bnode");
    let bnode_id = bnode.get("@id").and_then(|v| v.as_str()).unwrap_or("");
    assert!(bnode_id.starts_with("_:"));

    assert_eq!(result.get("ex:IRI"), Some(&json!({"@id": "ex:Abcdefg"})));
    assert_eq!(result.get("ex:isIRI"), Some(&json!(true)));
    assert_eq!(result.get("ex:isLiteral"), Some(&json!(true)));
    assert_eq!(result.get("ex:lang"), Some(&json!("es")));
    assert_eq!(result.get("ex:datatype"), Some(&json!("rdf:langString")));
    assert_eq!(
        result.get("ex:strdt"),
        Some(&json!({"@value": "Abcdefg", "@type": "ex:mystring"}))
    );
    assert_eq!(
        result.get("ex:strLang"),
        Some(&json!({"@value": "Abcdefg", "@language": "foo"}))
    );
}

#[tokio::test]
async fn update_where_bind_error_handling_unknown_function() {
    let fluree = FlureeBuilder::memory().build_memory();

    let db0 = Db::genesis("it/transact-update:error-handling-parse");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));
    let ledger_for_update = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns")
        .ledger;
    let parse_err = fluree
        .update(
            ledger_for_update,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(foo ?text)"]
                ],
                "insert": {"id":"?s","ex:text":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(
        parse_err.is_err(),
        "expected parse error for unknown function"
    );
    if let Err(err) = parse_err {
        assert!(
            err.to_string().contains("Unknown function: foo"),
            "unexpected error: {}",
            err
        );
    }

    let db0 = Db::genesis("it/transact-update:error-handling-query");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));
    let ledger_for_query = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns")
        .ledger;
    let query_err = fluree
        .query(
            &ledger_for_query,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"ex:error","ex:text":"?text"},
                    ["bind", "?err", "(foo ?text)"]
                ],
                "select": "?err"
            }),
        )
        .await;

    assert!(
        query_err.is_err(),
        "expected query parse error for unknown function"
    );
    if let Err(err) = query_err {
        assert!(
            err.to_string().contains("Unknown function: foo"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_runtime_type_mismatch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-runtime");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(abs ?text)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string().contains("bind evaluation failed"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_invalid_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-invalid-iri");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(iri \"bad:thing\")"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string().contains("Unknown IRI")
                || err.to_string().contains("Unknown IRI or namespace"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_invalid_datatype_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-invalid-dt-iri");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(str-dt ?text \"bad:datatype\")"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string().contains("Unknown datatype IRI"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_invalid_iri_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-iri-type");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(iri 42)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string()
                .contains("IRI requires a string or IRI argument"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_strdt_non_string() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-strdt-non-string");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(str-dt 42 \"xsd:string\")"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string()
                .contains("STRDT requires a string lexical form"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_bnode_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-bnode-arity");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(bnode ?text)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string().contains("BNODE requires no arguments"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_strlang_non_string() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-strlang-non-string");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(str-lang 42 \"en\")"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string()
                .contains("STRLANG requires a string lexical form"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_iri_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-iri-arity");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(iri ?text ?text)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string().contains("IRI requires exactly 1 argument"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_strdt_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-strdt-arity");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(str-dt ?text)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string()
                .contains("STRDT requires exactly 2 arguments"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_strlang_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-strlang-arity");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let run_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(str-lang ?text)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(run_err.is_err(), "expected runtime bind error");
    if let Err(err) = run_err {
        assert!(
            err.to_string()
                .contains("STRLANG requires exactly 2 arguments"),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn update_where_bind_error_handling_in_requires_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = Db::genesis("it/transact-update:error-in-list");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx_ex(),
                "insert": {
                    "@graph": [
                        {"id":"ex:create-predicates","ex:text":0,"ex:error":0},
                        {"id":"ex:error","ex:text":"Abcdefg"}
                    ]
                }
            }),
        )
        .await
        .expect("seed error fns");

    let parse_err = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex(),
                "where": [
                    {"id":"?s","ex:text":"?text"},
                    ["bind", "?err", "(in ?text 1)"]
                ],
                "insert": {"id":"?s","ex:error":"?err"},
                "values": ["?s", [{"@value":"ex:error","@type":"@id"}]]
            }),
        )
        .await;

    assert!(parse_err.is_err(), "expected parse error for in list");
    if let Err(err) = parse_err {
        assert!(
            err.to_string().contains("in requires a list literal"),
            "unexpected error: {}",
            err
        );
    }
}
