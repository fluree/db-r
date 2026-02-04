//! Credential verification for Fluree DB
//!
//! This crate provides verification of signed queries and transactions using:
//! - JWS (JSON Web Signature) compact format
//! - VerifiableCredential format (feature-gated, requires JSON-LD canonization)
//!
//! # JWS Format
//!
//! Compact serialization: `header.payload.signature`
//! - Header must contain `{"alg":"EdDSA","jwk":{...}}` with embedded Ed25519 public key
//! - Payload is base64url-encoded (JSON query/txn or raw SPARQL string)
//! - Signature is Ed25519 over `header.payload`
//!
//! # VerifiableCredential Format (requires "vc" feature)
//!
//! W3C VC with detached JWS proof. Requires JSON-LD URDNA2015 canonization
//! which is not yet implemented.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_credential::{verify, CredentialInput};
//!
//! // Verify a JWS string
//! let jws = "eyJhbGciOiJFZERTQSIsImp3ayI6ey4uLn19.eyJzZWxlY3QiOlsiP3MiXX0.c2ln...";
//! let result = verify(CredentialInput::Jws(jws))?;
//! println!("Identity: {}", result.did);
//! println!("Query: {}", result.subject);
//! ```

mod did;
mod ed25519;
pub mod error;
mod jws;
pub mod jwt_claims;

pub use did::{did_from_pubkey, pubkey_from_did};
pub use error::{CredentialError, Result};
pub use jws::{verify_jws, JwsVerified};
pub use jwt_claims::{ClaimsError, EventsTokenPayload};

use serde_json::Value as JsonValue;

/// Input type for verify() - accepts either JWS string or JSON
#[derive(Debug, Clone)]
pub enum CredentialInput<'a> {
    /// Compact JWS string (header.payload.signature)
    Jws(&'a str),
    /// JSON object (VerifiableCredential or JSON-wrapped JWS)
    Json(&'a JsonValue),
}

/// Unified result for credential verification
#[derive(Debug, Clone)]
pub struct VerifiedCredential {
    /// Decoded subject (JSON-parsed payload for JWS, credentialSubject for VC)
    pub subject: JsonValue,
    /// Signing identity (did:key:z6Mk...)
    pub did: String,
    /// Envelope @context (only for VerifiableCredentials)
    pub parent_context: Option<JsonValue>,
}

/// Verify a credential and extract subject + identity
///
/// # Input Types
/// - `CredentialInput::Jws(str)`: Verifies JWS, JSON-parses payload into subject
/// - `CredentialInput::Json(value)`: If string value, treats as JWS; otherwise VC (requires "vc" feature)
///
/// # Returns
/// `VerifiedCredential` with subject JSON, DID identity, and optional parent context
///
/// # Errors
/// - Various `CredentialError` variants for format/signature/identity issues
/// - `VcNotEnabled` if JSON input is a VC object but "vc" feature is not enabled
pub fn verify(input: CredentialInput<'_>) -> Result<VerifiedCredential> {
    match input {
        CredentialInput::Jws(jws) => verify_jws_to_credential(jws),
        CredentialInput::Json(value) => {
            // Check if it's a JSON string (wrapped JWS)
            if let Some(jws) = value.as_str() {
                return verify_jws_to_credential(jws);
            }

            // Otherwise it's a VC object
            verify_vc_object(value)
        }
    }
}

/// Verify JWS and convert to VerifiedCredential (JSON-parsing payload)
fn verify_jws_to_credential(jws: &str) -> Result<VerifiedCredential> {
    let jws_result = verify_jws(jws)?;

    // JSON-parse the payload
    let subject: JsonValue = serde_json::from_str(&jws_result.payload)?;

    Ok(VerifiedCredential {
        subject,
        did: jws_result.did,
        parent_context: None,
    })
}

/// Verify a VerifiableCredential JSON object
#[allow(unused_variables)]
fn verify_vc_object(credential: &JsonValue) -> Result<VerifiedCredential> {
    #[cfg(feature = "vc")]
    {
        // VC verification requires JSON-LD URDNA2015 canonization
        // This is NOT implementable with the current Rust json-ld crate
        // which only provides RFC 8785 JSON canonicalization.
        //
        // For now, return an error indicating VC support needs additional work.
        Err(CredentialError::VcNotEnabled)
    }

    #[cfg(not(feature = "vc"))]
    {
        Err(CredentialError::VcNotEnabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::{Signer, SigningKey};

    fn create_test_jws(payload: &str, signing_key: &SigningKey) -> String {
        let pubkey = signing_key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        let header = serde_json::json!({
            "alg": "EdDSA",
            "jwk": {
                "kty": "OKP",
                "crv": "Ed25519",
                "x": pubkey_b64
            }
        });

        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
    }

    #[test]
    fn test_verify_jws_input() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"select":["?s"],"where":{"@id":"?s"}}"#;

        let jws = create_test_jws(payload, &signing_key);
        let result = verify(CredentialInput::Jws(&jws)).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(
            result.subject.get("select").unwrap(),
            &serde_json::json!(["?s"])
        );
        assert!(result.parent_context.is_none());
    }

    #[test]
    fn test_verify_json_string_input() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"from":"ledger:test","select":["?s"]}"#;

        let jws = create_test_jws(payload, &signing_key);
        let json_value = JsonValue::String(jws);

        let result = verify(CredentialInput::Json(&json_value)).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(result.subject.get("from").unwrap(), "ledger:test");
    }

    #[test]
    fn test_verify_json_object_without_vc_feature() {
        let vc = serde_json::json!({
            "@context": "https://www.w3.org/2018/credentials/v1",
            "type": ["VerifiableCredential"],
            "credentialSubject": {"select": ["?s"]},
            "proof": {"jws": "..."}
        });

        let result = verify(CredentialInput::Json(&vc));
        assert!(matches!(result, Err(CredentialError::VcNotEnabled)));
    }

    #[test]
    fn test_verify_jws_invalid_json_payload() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);

        // Create JWS with non-JSON payload
        let jws = create_test_jws("not json {{", &signing_key);

        let result = verify(CredentialInput::Jws(&jws));
        assert!(matches!(result, Err(CredentialError::JsonParse(_))));
    }

    #[test]
    fn test_did_derivation_matches_test_vector() {
        // Test vectors from fluree.crypto cross_platform_test.cljc
        let test_pubkey: [u8; 32] = [
            0xa8, 0xde, 0xf1, 0x2a, 0xd7, 0x36, 0xf8, 0x84, 0x0f, 0x83, 0x6a, 0x46, 0xc6, 0x6c,
            0x9f, 0x3e, 0x20, 0x15, 0xd1, 0xea, 0x2c, 0x69, 0xd5, 0x46, 0xc0, 0x50, 0xfe, 0xf7,
            0x46, 0xbd, 0x63, 0xb3,
        ];
        let expected_did = "did:key:z6MkqpTi7zUDy5nnSfpLf7SPGsepMNJAxRiH1jbCZbuaZoEz";

        let did = did_from_pubkey(&test_pubkey);
        assert_eq!(did, expected_did);
    }
}
