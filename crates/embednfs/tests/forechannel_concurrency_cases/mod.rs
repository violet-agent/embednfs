use std::time::Duration;

use embednfs_proto::{FATTR4_SIZE, NfsStat4, OP_DESTROY_SESSION, OP_GETATTR, OP_SEQUENCE};

use crate::common::*;

mod framing;
mod replay;
mod slots;

/// Compound that resolves `name` and stats it. `GETATTR` is the operation the
/// gated backend can block, so this is the unit of blocked/fast work.
fn getattr_compound(tag: &str, sessionid: &[u8; 16], seq: u32, slot: u32, name: &str) -> Vec<u8> {
    let seq_op = encode_sequence(sessionid, seq, slot);
    let rootfh_op = encode_putrootfh();
    let lookup_op = encode_lookup(name);
    let getattr_op = encode_getattr(&[FATTR4_SIZE]);
    encode_compound(tag, &[&seq_op, &rootfh_op, &lookup_op, &getattr_op])
}

/// Asserts a reply is a successful four-operation GETATTR compound and returns
/// the reply's XID.
fn assert_getattr_ok(resp: &mut bytes::Bytes) -> u32 {
    let (xid, accept_stat) = parse_rpc_reply_fields(resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(resp);
    assert_eq!(status, NfsStat4::Ok as u32, "compound status");
    assert_eq!(num_results, 4);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_SEQUENCE);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    skip_sequence_res(resp);
    let _ = parse_op_header(resp);
    let _ = parse_op_header(resp);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_GETATTR);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    xid
}

/// Asserts a reply is a successful `SEQUENCE; DESTROY_SESSION` compound and
/// returns the reply's XID.
fn assert_destroy_session_ok(resp: &mut bytes::Bytes) -> u32 {
    let (xid, accept_stat) = parse_rpc_reply_fields(resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(resp);
    assert_eq!(status, NfsStat4::Ok as u32, "compound status");
    assert_eq!(num_results, 2);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_SEQUENCE);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    skip_sequence_res(resp);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_DESTROY_SESSION);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    xid
}

/// Asserts a reply is a SEQUENCE-only error compound with `expected` status.
fn assert_sequence_error(resp: &mut bytes::Bytes, expected: NfsStat4) -> u32 {
    let (xid, accept_stat) = parse_rpc_reply_fields(resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(resp);
    assert_eq!(status, expected as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_SEQUENCE);
    assert_eq!(op_status, expected as u32);
    xid
}

/// Upper bound for "this must not have completed yet" assertions.
const NOT_YET_WINDOW: Duration = Duration::from_millis(250);
