use serde_json::{json, Value};

/// Generate the JSON-LD schema for the memory ledger.
///
/// This schema defines the `mem:` namespace classes and properties
/// used by the memory store.
pub fn memory_schema_jsonld() -> Value {
    json!({
        "@context": {
            "mem": "https://ns.flur.ee/memory#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Classes
            {
                "@id": "mem:Fact",
                "@type": "rdfs:Class",
                "rdfs:label": "Fact"
            },
            {
                "@id": "mem:Decision",
                "@type": "rdfs:Class",
                "rdfs:label": "Decision"
            },
            {
                "@id": "mem:Constraint",
                "@type": "rdfs:Class",
                "rdfs:label": "Constraint"
            },
            {
                "@id": "mem:Preference",
                "@type": "rdfs:Class",
                "rdfs:label": "Preference"
            },
            {
                "@id": "mem:Artifact",
                "@type": "rdfs:Class",
                "rdfs:label": "Artifact"
            },
            // Scope named-graph classes
            {
                "@id": "mem:repo",
                "@type": "rdfs:Class",
                "rdfs:label": "Repo scope"
            },
            {
                "@id": "mem:user",
                "@type": "rdfs:Class",
                "rdfs:label": "User scope"
            },
            // Properties
            {
                "@id": "mem:content",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:tag",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:scope",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "rdfs:Resource" }
            },
            {
                "@id": "mem:sensitivity",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:severity",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:artifactRef",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:branch",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:supersedes",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "rdfs:Resource" }
            },
            {
                "@id": "mem:validFrom",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:dateTime" }
            },
            {
                "@id": "mem:validTo",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:dateTime" }
            },
            {
                "@id": "mem:createdAt",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:dateTime" }
            },
            // Type-specific properties
            {
                "@id": "mem:rationale",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:alternatives",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:factKind",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:prefScope",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            },
            {
                "@id": "mem:artifactKind",
                "@type": "rdf:Property",
                "rdfs:range": { "@id": "xsd:string" }
            }
        ]
    })
}

/// Build a JSON-LD insert document from a Memory struct.
pub fn memory_to_jsonld(mem: &crate::types::Memory) -> Value {
    let mut node = json!({
        "@context": {
            "mem": "https://ns.flur.ee/memory#"
        },
        "@id": mem.id,
        "@type": mem.kind.class_iri(),
        "mem:content": { "@value": mem.content, "@type": "@fulltext" },
        "mem:scope": { "@id": mem.scope.prefixed() },
        "mem:sensitivity": mem.sensitivity.as_str(),
        "mem:createdAt": mem.created_at
    });

    let obj = node.as_object_mut().unwrap();

    if !mem.tags.is_empty() {
        if mem.tags.len() == 1 {
            obj.insert("mem:tag".to_string(), json!(mem.tags[0]));
        } else {
            obj.insert("mem:tag".to_string(), json!(mem.tags));
        }
    }

    if let Some(sev) = &mem.severity {
        let sev_str = match sev {
            crate::types::Severity::Must => "must",
            crate::types::Severity::Should => "should",
            crate::types::Severity::Prefer => "prefer",
        };
        obj.insert("mem:severity".to_string(), json!(sev_str));
    }

    if !mem.artifact_refs.is_empty() {
        if mem.artifact_refs.len() == 1 {
            obj.insert("mem:artifactRef".to_string(), json!(mem.artifact_refs[0]));
        } else {
            obj.insert("mem:artifactRef".to_string(), json!(mem.artifact_refs));
        }
    }

    if let Some(b) = &mem.branch {
        obj.insert("mem:branch".to_string(), json!(b));
    }

    if let Some(sup) = &mem.supersedes {
        obj.insert("mem:supersedes".to_string(), json!({ "@id": sup }));
    }

    if let Some(vf) = &mem.valid_from {
        obj.insert("mem:validFrom".to_string(), json!(vf));
    }

    if let Some(vt) = &mem.valid_to {
        obj.insert("mem:validTo".to_string(), json!(vt));
    }

    // Type-specific predicates
    if let Some(r) = &mem.rationale {
        obj.insert(
            "mem:rationale".to_string(),
            json!({ "@value": r, "@type": "@fulltext" }),
        );
    }
    if let Some(a) = &mem.alternatives {
        obj.insert("mem:alternatives".to_string(), json!(a));
    }
    if let Some(fk) = &mem.fact_kind {
        obj.insert("mem:factKind".to_string(), json!(fk));
    }
    if let Some(ps) = &mem.pref_scope {
        obj.insert("mem:prefScope".to_string(), json!(ps));
    }
    if let Some(ak) = &mem.artifact_kind {
        obj.insert("mem:artifactKind".to_string(), json!(ak));
    }

    node
}
