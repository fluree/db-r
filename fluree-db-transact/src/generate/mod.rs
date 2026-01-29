//! Flake generation from transaction templates
//!
//! This module provides functionality for materializing triple templates
//! into concrete flakes, handling variable substitution, blank node
//! skolemization, and assertion/retraction cancellation.

pub mod cancellation;
pub mod flakes;

pub use cancellation::apply_cancellation;
pub use flakes::{infer_datatype, FlakeGenerator};
