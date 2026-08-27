//! Shared test helpers for NFSv4.1 integration tests.
//!
//! Provides server setup, XDR encoding helpers, and response parsing utilities
//! so that individual test modules stay focused on test logic.
#![allow(
    dead_code,
    unused_imports,
    unreachable_pub,
    reason = "each test binary compiles only the helpers it uses"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test helpers assert by panicking; a failed unwrap is a failed test"
)]

mod attr_bits;
mod encode;
mod external_server;
mod fixtures;
mod gated_fs;
mod nfs4j;
mod nfs_rs;
mod parse;
mod server;
mod session;
mod transport;
mod wrappers;

pub use attr_bits::*;
pub use encode::*;
pub use external_server::*;
pub use fixtures::*;
pub use gated_fs::*;
pub use nfs_rs::*;
pub use nfs4j::*;
pub use parse::*;
pub use server::*;
pub use session::*;
pub use transport::*;
pub use wrappers::*;
