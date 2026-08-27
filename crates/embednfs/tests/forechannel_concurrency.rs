//! Tests for bounded concurrent forechannel request processing.
//!
//! One TCP connection carries every NFSv4.1 session slot, so these tests cover
//! how the record reader, the bounded request workers, and the response writer
//! interact: slot scheduling, the shared/exclusive control lane, replay
//! finalization across disconnects and worker faults, and record framing when
//! requests complete out of order.
//! The per-test `Origin:` and `RFC:` lines below are the authoritative
//! provenance.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test code asserts by panicking; a failed unwrap is a failed test"
)]

mod common;
mod forechannel_concurrency_cases;
