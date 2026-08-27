//! Tests for the forechannel reply sizes a session negotiates.
//!
//! CREATE_SESSION agrees on `ca_maxresponsesize` and
//! `ca_maxresponsesize_cached` (RFC 8881 §18.36.3). These cases cover what the
//! server does when a client then asks for more than either allows: a READ or
//! READDIR is answered short, and a reply too large for the slot's replay cache
//! is not stored — without ever letting the request execute twice.
//!
//! This file replaces the outbound record-fragmentation smoke that used to live
//! in `transport_fragmentation.rs`: a COMPOUND reply can no longer exceed one
//! RPC record fragment, so the writer's fragmentation loop is covered by
//! `server::transport::tests::test_a_reply_larger_than_the_fragment_size_is_split`.
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

use bytes::Bytes;

use embednfs_proto::{FATTR4_SIZE, NfsStat4, OP_PUTROOTFH, OP_READ, OP_READDIR, OP_SEQUENCE};

use crate::common::*;

/// `ca_maxresponsesize` small enough that a 64 KiB READ cannot fit it, and
/// large enough to leave room for real payload.
const SMALL_MAX_RESPONSE: u32 = 8192;
const FILE_BYTES: usize = 64 * 1024;

fn read_compound(tag: &str, sessionid: &[u8; 16], seq: u32, slot: u32, offset: u64) -> Vec<u8> {
    let seq_op = encode_sequence(sessionid, seq, slot);
    let rootfh_op = encode_putrootfh();
    let lookup_op = encode_lookup("big.bin");
    let read_op = encode_read(offset, FILE_BYTES as u32);
    encode_compound(tag, &[&seq_op, &rootfh_op, &lookup_op, &read_op])
}

/// Steps past `SEQUENCE; PUTROOTFH; LOOKUP` and returns the READ result.
fn parse_read_compound(resp: &mut Bytes) -> (bool, Bytes) {
    parse_rpc_reply(resp);
    let (status, _, num_results) = parse_compound_header(resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 4);
    let _ = parse_op_header(resp);
    skip_sequence_res(resp);
    let _ = parse_op_header(resp);
    let _ = parse_op_header(resp);
    let (opnum, op_status) = parse_op_header(resp);
    assert_eq!(opnum, OP_READ);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    parse_read_res(resp)
}

/// A READ larger than the session's `ca_maxresponsesize` is answered short
/// rather than overrunning the reply, and the client reads the rest with a
/// second READ.
/// Origin: review of `op_read` forwarding the client's `count` to the backend unclamped.
/// RFC: RFC 8881 §18.36.3 (ca_maxresponsesize), §2.10.6.4, §18.22.3 (a READ may return fewer bytes).
#[tokio::test]
async fn test_read_beyond_maxresponsesize_is_answered_short() {
    let payload = vec![0x5a; FILE_BYTES];
    let fs = fs_with_data("big.bin", &payload).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid =
        setup_session_with_fore_limits(&mut stream, b"short-read", SMALL_MAX_RESPONSE, 8192).await;

    let compound = read_compound("short-read", &sessionid, 1, 0, 0);
    let (mut resp, fragments) = send_rpc_record(&mut stream, 3, 1, &compound).await;
    assert_eq!(fragments, 1, "a bounded reply fits one record");
    // The record marking header is excluded from ca_maxresponsesize
    // (RFC 8881 §18.36.3), so the reassembled record is what the limit covers.
    let reply_len = resp.len();
    assert!(
        reply_len <= SMALL_MAX_RESPONSE as usize,
        "reply of {reply_len} bytes exceeds the negotiated {SMALL_MAX_RESPONSE}"
    );

    let (eof, data) = parse_read_compound(&mut resp);
    assert!(!data.is_empty(), "the clamp must still return useful data");
    assert!(data.len() < FILE_BYTES, "the READ was answered short");
    assert!(!eof, "a short read in the middle of the file is not eof");
    assert!(data.iter().all(|byte| *byte == 0x5a));

    // The remainder is still readable: the clamp shortened the reply, it did
    // not truncate the file.
    let next = read_compound("short-read-2", &sessionid, 2, 0, data.len() as u64);
    let (mut resp, _) = send_rpc_record(&mut stream, 4, 1, &next).await;
    let (_, more) = parse_read_compound(&mut resp);
    assert!(!more.is_empty(), "the rest of the file is still readable");
    assert!(more.iter().all(|byte| *byte == 0x5a));
}

/// A READDIR whose `maxcount` exceeds the session's `ca_maxresponsesize` is
/// answered with as many entries as the negotiated reply holds, not with the
/// whole directory.
/// Origin: review of `op_readdir` sizing its backend request from the client's `maxcount`.
/// RFC: RFC 8881 §18.36.3 (ca_maxresponsesize), §18.23.3 (maxcount bounds READDIR4resok).
#[tokio::test]
async fn test_readdir_maxcount_is_bounded_by_maxresponsesize() {
    let names: Vec<String> = (0..60).map(|i| format!("entry-{i:03}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let port = start_server_with_fs(populated_fs(&name_refs).await).await;

    let mut stream = connect(port).await;
    let bounded = setup_session_with_fore_limits(&mut stream, b"bounded-readdir", 4096, 4096).await;

    let readdir_op = encode_readdir_custom(0, [0u8; 8], 0, u32::MAX, &[FATTR4_SIZE]);
    let compound = encode_compound(
        "bounded-readdir",
        &[
            &encode_sequence(&bounded, 1, 0),
            &encode_putrootfh(),
            &readdir_op,
        ],
    );
    let (mut resp, _) = send_rpc_record(&mut stream, 3, 1, &compound).await;
    let reply_len = resp.len();
    assert!(
        reply_len <= 4096,
        "reply of {reply_len} bytes exceeds the negotiated 4096"
    );
    parse_rpc_reply(&mut resp);
    let (status, _, _) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    let _ = parse_op_header(&mut resp);
    skip_sequence_res(&mut resp);
    let _ = parse_op_header(&mut resp);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_READDIR);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    let (_, _, bounded_entries, bounded_eof) = parse_readdir_body(&mut resp);
    assert!(!bounded_entries.is_empty(), "some entries still fit");
    assert!(!bounded_eof, "the directory could not be finished");
    assert!(bounded_entries.len() < names.len());

    // The identical READDIR on a session with the default limits returns more,
    // which is what shows the ceiling came from the negotiated reply size.
    let mut roomy_stream = connect(port).await;
    let roomy = setup_session(&mut roomy_stream).await;
    let compound = encode_compound(
        "roomy-readdir",
        &[
            &encode_sequence(&roomy, 1, 0),
            &encode_putrootfh(),
            &readdir_op,
        ],
    );
    let (mut resp, _) = send_rpc_record(&mut roomy_stream, 3, 1, &compound).await;
    parse_rpc_reply(&mut resp);
    let _ = parse_compound_header(&mut resp);
    let _ = parse_op_header(&mut resp);
    skip_sequence_res(&mut resp);
    let _ = parse_op_header(&mut resp);
    let _ = parse_op_header(&mut resp);
    let (_, _, roomy_entries, _) = parse_readdir_body(&mut resp);
    assert!(
        roomy_entries.len() > bounded_entries.len(),
        "a larger negotiated reply must fit more entries"
    );
}

/// A reply too large for `ca_maxresponsesize_cached` is not stored in the slot,
/// and the retry is answered with NFS4ERR_RETRY_UNCACHED_REP on the operation
/// after SEQUENCE instead of executing the request a second time.
/// Origin: review of `finish_sequence` caching an arbitrarily large encoded body per slot.
/// RFC: RFC 8881 §2.10.6.1.3 (uncached reply, and no NFS4ERR_RETRY_UNCACHED_REP on a leading SEQUENCE), §18.36.3.
#[tokio::test]
async fn test_reply_too_large_to_cache_is_not_replayed_or_re_executed() {
    let (fs, gate) = gated_fs(&[("file.txt", 0)], &[], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    // Every reply is larger than this, so nothing this session sends is
    // cacheable.
    let sessionid = setup_session_with_fore_limits(&mut stream, b"uncached", 8192, 16).await;

    let compound = encode_compound(
        "uncached",
        &[
            &encode_sequence(&sessionid, 1, 0),
            &encode_putrootfh(),
            &encode_lookup("file.txt"),
            &encode_getattr(&[FATTR4_SIZE]),
        ],
    );
    let (mut resp, _) = send_rpc_record(&mut stream, 3, 1, &compound).await;
    parse_rpc_reply(&mut resp);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32, "the original request succeeds");
    assert_eq!(num_results, 4);
    let executed_calls = gate.getattr_calls();
    assert!(executed_calls > 0);

    // The same slot and sequence id: a retry, not a new request.
    let (mut retry, _) = send_rpc_record(&mut stream, 4, 1, &compound).await;
    let (xid, accept_stat) = parse_rpc_reply_fields(&mut retry);
    assert_eq!(xid, 4);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut retry);
    assert_eq!(status, NfsStat4::RetryUncachedRep as u32);
    assert_eq!(num_results, 2, "SEQUENCE plus the operation after it");
    let (opnum, op_status) = parse_op_header(&mut retry);
    assert_eq!(opnum, OP_SEQUENCE);
    assert_eq!(
        op_status,
        NfsStat4::Ok as u32,
        "a leading SEQUENCE must not carry NFS4ERR_RETRY_UNCACHED_REP"
    );
    skip_sequence_res(&mut retry);
    let (opnum, op_status) = parse_op_header(&mut retry);
    assert_eq!(opnum, OP_PUTROOTFH);
    assert_eq!(op_status, NfsStat4::RetryUncachedRep as u32);

    assert_eq!(
        gate.getattr_calls(),
        executed_calls,
        "an uncached retry must not re-execute the request"
    );

    // A *different* request on that slot and sequence id is still a false
    // retry, which is what shows the slot was consumed rather than freed.
    let different = encode_compound(
        "uncached-different",
        &[
            &encode_sequence(&sessionid, 1, 0),
            &encode_putrootfh(),
            &encode_getattr(&[FATTR4_SIZE]),
        ],
    );
    let (mut resp, _) = send_rpc_record(&mut stream, 5, 1, &different).await;
    parse_rpc_reply(&mut resp);
    let (status, _, _) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::SeqFalseRetry as u32);
}

/// Two sessions on one connection are each held to their own negotiated reply
/// size: the smaller session's limit never bounds the larger one, and the
/// unsequenced CREATE_SESSION that establishes the second session is not
/// bounded by the first either.
/// Origin: review requirement that a session's limit not leak onto other work sharing the connection.
/// RFC: RFC 8881 §2.10.6.4, §18.36.3 (limits are per channel of a session).
#[tokio::test]
async fn test_reply_limits_are_per_session_not_per_connection() {
    let payload = vec![0x5a; FILE_BYTES];
    let fs = fs_with_data("big.bin", &payload).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;

    let (clientid, seq) = exchange_id_only(&mut stream, 1, b"two-sessions").await;
    let small =
        create_session_with_fore_limits(&mut stream, 2, clientid, seq, SMALL_MAX_RESPONSE, 8192)
            .await;
    // Created while the small session is live on the same connection.
    let large =
        create_session_with_fore_limits(&mut stream, 3, clientid, seq + 1, 1_048_576, 8192).await;

    let compound = read_compound("small-session", &small, 1, 0, 0);
    let (mut resp, _) = send_rpc_record(&mut stream, 4, 1, &compound).await;
    assert!(resp.len() <= SMALL_MAX_RESPONSE as usize);
    let (_, small_data) = parse_read_compound(&mut resp);
    assert!(small_data.len() < FILE_BYTES);

    let compound = read_compound("large-session", &large, 1, 0, 0);
    let (mut resp, _) = send_rpc_record(&mut stream, 5, 1, &compound).await;
    let (eof, large_data) = parse_read_compound(&mut resp);
    assert_eq!(
        large_data.len(),
        FILE_BYTES,
        "the larger session's READ must not be clamped by the smaller session"
    );
    assert!(eof);
}
