use std::time::Duration;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::time::timeout;

use embednfs_proto::NfsStat4;

use super::{NOT_YET_WINDOW, assert_getattr_ok, assert_sequence_error, getattr_compound};
use crate::common::*;

/// Sends `compound` until the slot stops answering `NFS4ERR_DELAY`, i.e. until
/// the worker that owns the slot has finalized its replay cache entry.
async fn retry_until_final(stream: &mut TcpStream, compound: &[u8], first_xid: u32) -> Bytes {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut xid = first_xid;
    loop {
        write_rpc_record(stream, xid, 1, compound).await;
        let (resp, _) = timeout(Duration::from_secs(10), read_rpc_record(stream))
            .await
            .expect("retry reply");

        let mut probe = resp.clone();
        let (reply_xid, _) = parse_rpc_reply_fields(&mut probe);
        assert_eq!(reply_xid, xid);
        let (status, _, _) = parse_compound_header(&mut probe);
        if status != NfsStat4::Delay as u32 {
            return resp;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "slot never left the in-progress state"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        xid += 1;
    }
}

/// A retransmission of a completed request replays the exact cached COMPOUND
/// body under the retry's own RPC XID and does not re-run the operation.
/// Origin: `pynfs/nfs4.1/server41tests/st_sequence.py` (`SEQ` replay cases); RFC 8881 §2.10.6.1.3.
/// RFC: RFC 8881 §2.10.6.1.3, §18.46.3.
#[tokio::test]
async fn test_completed_retransmission_replays_cached_body_under_new_xid() {
    let (fs, gate) = gated_fs(&[("file.txt", 0)], &[], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let compound = getattr_compound("replay", &sessionid, 1, 0, "file.txt");
    let (original, _) = {
        write_rpc_record(&mut stream, 3, 1, &compound).await;
        read_rpc_record(&mut stream).await
    };
    let executed_calls = gate.getattr_calls();
    assert!(executed_calls > 0);

    let (retry, _) = {
        write_rpc_record(&mut stream, 4, 1, &compound).await;
        read_rpc_record(&mut stream).await
    };

    let mut original_body = original.clone();
    let (original_xid, _) = parse_rpc_reply_fields(&mut original_body);
    let mut retry_body = retry.clone();
    let (retry_xid, _) = parse_rpc_reply_fields(&mut retry_body);

    assert_eq!(original_xid, 3);
    assert_eq!(retry_xid, 4, "the replay carries the retry's XID");
    assert_eq!(
        original_body, retry_body,
        "the replay must be the byte-identical cached COMPOUND body"
    );
    assert_eq!(
        gate.getattr_calls(),
        executed_calls,
        "a replay must not re-execute the request"
    );

    let mut checked = retry.clone();
    assert_eq!(assert_getattr_ok(&mut checked), 4);
}

/// Disconnecting after the request was dispatched but before its reply is
/// written still leaves a valid replay entry: the client reconnects and the
/// retry replays the executed result without running it twice.
/// Origin: RFC 8881 §2.10.6.1.3 combined with connection loss during execution.
/// RFC: RFC 8881 §2.10.6.1.3, §2.10.6.2.
#[tokio::test]
async fn test_disconnect_after_dispatch_leaves_valid_replay_entry() {
    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let compound = getattr_compound("disconnect", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &compound).await;
    gate.wait_entered(1).await;

    // The client vanishes while the backend is still working.
    std::mem::drop(stream);
    gate.release();

    let mut reconnected = connect(port).await;
    let mut resp = retry_until_final(&mut reconnected, &compound, 4).await;
    let xid = assert_getattr_ok(&mut resp);
    assert!(xid >= 4);
    assert_eq!(
        gate.entered(),
        1,
        "the replayed retry must not execute the blocked GETATTR again"
    );
}

/// A failed response write (peer reset) does not strand the slot, the worker,
/// or the session: the executed result stays replayable and the server keeps
/// serving new requests on the same session.
/// Origin: implementation-driven cleanup check for the dedicated response writer.
/// RFC: RFC 8881 §2.10.6.1.3, §2.10.6.2.
#[tokio::test]
#[expect(
    deprecated,
    reason = "SO_LINGER(0) is the deterministic way to make the peer reset the connection so the pending response write fails; a zero linger never blocks on drop"
)]
async fn test_writer_failure_does_not_leak_workers_or_slots() {
    let (fs, gate) = gated_fs(&[("block.txt", 0), ("fast.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_limit(fs, 2).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let compound = getattr_compound("writer-failure", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &compound).await;
    gate.wait_entered(1).await;

    // Reset instead of a graceful close so the pending response write fails.
    stream.set_linger(Some(Duration::ZERO)).unwrap();
    std::mem::drop(stream);
    gate.release();

    let mut reconnected = connect(port).await;
    let mut resp = retry_until_final(&mut reconnected, &compound, 4).await;
    let _ = assert_getattr_ok(&mut resp);

    // The connection limit was 2; run more than that many further requests to
    // show no permit or session lock was leaked with the reset connection.
    for slot in 1..6u32 {
        let next = getattr_compound("after-reset", &sessionid, 1, slot, "fast.txt");
        write_rpc_record(&mut reconnected, 100 + slot, 1, &next).await;
        let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut reconnected))
            .await
            .expect("server still serves requests after a writer failure");
        assert_eq!(assert_getattr_ok(&mut resp), 100 + slot);
    }
}

/// A worker that panics after `prepare_sequence` finalizes the slot with a
/// replayable NFS4ERR_SERVERFAULT instead of leaving it in progress, and the
/// connection keeps serving other slots.
/// Origin: cancellation/panic safety requirement for the prepare/finish window.
/// RFC: RFC 8881 §2.10.6.1.3 (slot state after a failed request), §15.1.
#[tokio::test]
async fn test_worker_panic_after_prepare_leaves_replayable_fault() {
    let (fs, gate) = gated_fs(&[("boom.txt", 0), ("ok.txt", 0)], &[], &["boom.txt"]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let compound = getattr_compound("panic", &sessionid, 1, 0, "boom.txt");
    write_rpc_record(&mut stream, 3, 1, &compound).await;
    // The worker dies mid-request, so this XID is never answered.
    assert!(
        timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream))
            .await
            .is_err(),
        "a panicking worker cannot produce a reply"
    );

    let mut resp = retry_until_final(&mut stream, &compound, 4).await;
    let xid = assert_sequence_error(&mut resp, NfsStat4::Serverfault);
    assert!(xid >= 4);

    // A *different* request reusing that slot and sequence id is rejected as a
    // false retry, which proves the slot holds a cached reply instead of having
    // been silently made reusable after the panic.
    let different = getattr_compound("panic-different", &sessionid, 1, 0, "ok.txt");
    write_rpc_record(&mut stream, 40, 1, &different).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("false retry reply");
    assert_eq!(
        assert_sequence_error(&mut resp, NfsStat4::SeqFalseRetry),
        40
    );

    // Other slots on the same connection are unaffected.
    let healthy = getattr_compound("healthy", &sessionid, 1, 1, "ok.txt");
    write_rpc_record(&mut stream, 50, 1, &healthy).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("healthy slot reply");
    assert_eq!(assert_getattr_ok(&mut resp), 50);
    assert!(gate.getattr_calls() > 0);
}
