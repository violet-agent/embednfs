use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::timeout;

use embednfs_proto::{NfsStat4, OP_EXCHANGE_ID};

use super::{
    NOT_YET_WINDOW, assert_destroy_session_ok, assert_getattr_ok, assert_sequence_error,
    getattr_compound,
};
use crate::common::*;

const SLOW_GETATTR: Duration = Duration::from_secs(5);

/// A GETATTR blocked for five seconds on one slot does not delay a GETATTR on
/// another slot of the same session and connection; the fast reply lands in
/// well under 250 ms.
/// Origin: acceptance benchmark for bounded concurrent forechannel processing (macOS drives many slots over one TCP connection).
/// RFC: RFC 8881 §2.10.6.1, §18.46.
#[tokio::test]
async fn test_slow_slot_does_not_block_other_slot() {
    let (fs, gate) = gated_fs(&[("block.txt", 0), ("fast.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let blocked = getattr_compound("slow-slot", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &blocked).await;
    // Deterministic: the backend is inside the blocked GETATTR.
    gate.wait_entered(1).await;

    let releaser = Arc::clone(&gate);
    let blocked_started = Instant::now();
    std::mem::drop(tokio::spawn(async move {
        tokio::time::sleep(SLOW_GETATTR).await;
        releaser.release();
    }));

    let fast = getattr_compound("fast-slot", &sessionid, 1, 1, "fast.txt");
    let started = Instant::now();
    write_rpc_record(&mut stream, 4, 1, &fast).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream))
        .await
        .expect("fast slot reply must arrive without waiting for the blocked slot");
    let fast_latency = started.elapsed();
    println!("bounded concurrency: fast slot replied in {fast_latency:?}");
    assert_eq!(assert_getattr_ok(&mut resp), 4, "fast reply xid");
    assert!(
        fast_latency < NOT_YET_WINDOW,
        "fast slot latency {fast_latency:?} must stay under {NOT_YET_WINDOW:?}"
    );

    let (mut resp, _) = timeout(SLOW_GETATTR * 3, read_rpc_record(&mut stream))
        .await
        .expect("blocked slot reply");
    assert_eq!(assert_getattr_ok(&mut resp), 3, "blocked reply xid");
    assert!(
        blocked_started.elapsed() >= SLOW_GETATTR,
        "the blocked request really did take at least {SLOW_GETATTR:?}"
    );
}

/// With per-connection concurrency pinned to one, the same workload is
/// head-of-line blocked: the second slot's reply waits for the first to finish.
/// This is the pre-change behavior of the read/execute/write loop, kept as the
/// benchmark's control case.
/// Origin: control case for the bounded concurrency benchmark; reproduces the serialized handler.
/// RFC: RFC 8881 §2.10.6.1.
#[tokio::test]
async fn test_serialized_connection_head_of_line_blocks_other_slot() {
    const BLOCK_FOR: Duration = Duration::from_millis(600);

    let (fs, gate) = gated_fs(&[("block.txt", 0), ("fast.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_limit(fs, 1).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let blocked = getattr_compound("serial-slow", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &blocked).await;
    gate.wait_entered(1).await;

    let releaser = Arc::clone(&gate);
    std::mem::drop(tokio::spawn(async move {
        tokio::time::sleep(BLOCK_FOR).await;
        releaser.release();
    }));

    let fast = getattr_compound("serial-fast", &sessionid, 1, 1, "fast.txt");
    let started = Instant::now();
    write_rpc_record(&mut stream, 4, 1, &fast).await;

    // The blocked request holds the only execution permit, so its reply is
    // written first and the fast reply cannot arrive early.
    let (mut resp, _) = timeout(BLOCK_FOR * 5, read_rpc_record(&mut stream))
        .await
        .expect("blocked reply");
    assert_eq!(assert_getattr_ok(&mut resp), 3);
    let (mut resp, _) = timeout(BLOCK_FOR * 5, read_rpc_record(&mut stream))
        .await
        .expect("fast reply");
    assert_eq!(assert_getattr_ok(&mut resp), 4);
    println!(
        "serialized connection: fast slot replied in {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() >= BLOCK_FOR / 2,
        "serialized handling must delay the fast slot"
    );
}

/// Two requests on one slot never execute the filesystem operation
/// concurrently: advancing the sequence id while the slot is still executing
/// returns `NFS4ERR_DELAY` and never reaches the backend.
/// Origin: RFC 8881 §2.10.6.1 (one outstanding request per slot); implementation-driven concurrency check.
/// RFC: RFC 8881 §2.10.6.1, §18.46.3.
#[tokio::test]
async fn test_same_slot_requests_never_run_concurrently() {
    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let first = getattr_compound("slot0-first", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &first).await;
    gate.wait_entered(1).await;

    let second = getattr_compound("slot0-second", &sessionid, 2, 0, "block.txt");
    write_rpc_record(&mut stream, 4, 1, &second).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream))
        .await
        .expect("busy slot must be answered immediately");
    assert_eq!(assert_sequence_error(&mut resp, NfsStat4::Delay), 4);
    assert_eq!(
        gate.entered(),
        1,
        "the second request must not reach the filesystem"
    );

    gate.release();
    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("first reply");
    assert_eq!(assert_getattr_ok(&mut resp), 3);
    assert_eq!(gate.max_inflight(), 1, "slot 0 executed one op at a time");

    // Once the slot is idle the client may advance the sequence id.
    write_rpc_record(&mut stream, 5, 1, &second).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("retry after completion");
    assert_eq!(assert_getattr_ok(&mut resp), 5);
    assert_eq!(gate.max_inflight(), 1);
}

/// Retransmitting the in-flight request on the same slot returns
/// `NFS4ERR_DELAY` while the original keeps executing.
/// Origin: RFC 8881 §2.10.6.1.3 (retry of an in-progress request); same-connection variant of the existing two-connection test.
/// RFC: RFC 8881 §2.10.6.1.3.
#[tokio::test]
async fn test_same_slot_retransmission_during_execution_returns_delay() {
    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let request = getattr_compound("retry-delay", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &request).await;
    gate.wait_entered(1).await;

    // Byte-identical retransmission under a new XID.
    write_rpc_record(&mut stream, 4, 1, &request).await;
    let (mut resp, _) = timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream))
        .await
        .expect("retransmission must be answered while the original executes");
    assert_eq!(assert_sequence_error(&mut resp, NfsStat4::Delay), 4);
    assert_eq!(gate.entered(), 1, "no second execution");

    gate.release();
    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("original reply");
    assert_eq!(assert_getattr_ok(&mut resp), 3);
}

/// A large pipelined workload never exceeds the configured per-connection
/// concurrency limit, and every request still completes.
/// Origin: implementation-driven bound check for the request worker pool.
/// RFC: RFC 8881 §2.10.6.1 (slot table bounds outstanding requests).
#[tokio::test]
async fn test_concurrency_limit_bounds_in_flight_requests() {
    const LIMIT: usize = 3;
    const REQUESTS: u32 = 8; // the test session advertises 8 forechannel slots

    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_limit(fs, LIMIT).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let mut pending = HashSet::new();
    for slot in 0..REQUESTS {
        let xid = 100 + slot;
        let compound = getattr_compound("bounded", &sessionid, 1, slot, "block.txt");
        write_rpc_record(&mut stream, xid, 1, &compound).await;
        let _ = pending.insert(xid);
    }

    gate.wait_entered(LIMIT).await;
    tokio::time::sleep(NOT_YET_WINDOW).await;
    assert_eq!(
        gate.entered(),
        LIMIT,
        "only {LIMIT} requests may be dispatched at once"
    );
    assert!(gate.max_inflight() <= LIMIT);

    gate.release();
    for _ in 0..REQUESTS {
        let (mut resp, _) = timeout(NOT_YET_WINDOW * 40, read_rpc_record(&mut stream))
            .await
            .expect("every pipelined request completes");
        let xid = assert_getattr_ok(&mut resp);
        assert!(pending.remove(&xid), "unexpected or duplicated xid {xid}");
    }
    assert!(pending.is_empty());
    assert!(
        gate.max_inflight() <= LIMIT,
        "peak concurrency {} exceeded the limit",
        gate.max_inflight()
    );
}

/// A COMPOUND without a leading SEQUENCE takes the control lane exclusively: it
/// waits for the in-flight slot worker instead of running beside it.
/// Origin: conservative control-lane requirement for session creation/destruction under concurrent slot execution.
/// RFC: RFC 8881 §2.10.6.1, §18.35 (EXCHANGE_ID outside a session).
#[tokio::test]
async fn test_control_compound_is_exclusive_with_slot_workers() {
    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let blocked = getattr_compound("control-slot", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &blocked).await;
    gate.wait_entered(1).await;

    let exchange_id_op = encode_exchange_id_with_name(b"control-lane-client");
    let control = encode_compound("control", &[&exchange_id_op]);
    write_rpc_record(&mut stream, 4, 1, &control).await;
    assert!(
        timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream))
            .await
            .is_err(),
        "control compound must not execute while a slot worker holds the shared gate"
    );

    gate.release();
    let (mut slot_resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("slot reply");
    assert_eq!(assert_getattr_ok(&mut slot_resp), 3);

    let (mut control_resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("control reply");
    let (xid, accept_stat) = parse_rpc_reply_fields(&mut control_resp);
    assert_eq!(xid, 4);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut control_resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut control_resp);
    assert_eq!(opnum, OP_EXCHANGE_ID);
    assert_eq!(op_status, NfsStat4::Ok as u32);
}

/// A COMPOUND that opens with a valid SEQUENCE but ends in DESTROY_SESSION is
/// still a control request: it waits for the in-flight slot worker instead of
/// destroying the session out from under it. The session is therefore still
/// alive while that worker executes, so the worker's slot finalization cannot
/// fail with `NFS4ERR_BADSESSION` and lose its executed reply.
/// Origin: lane-selection review of the concurrent forechannel loop; a leading SEQUENCE alone does not make a COMPOUND safe to run beside other slots.
/// RFC: RFC 8881 §18.37.3 (DESTROY_SESSION as the final operation of a sequenced COMPOUND), §2.10.6.1.3.
#[tokio::test]
async fn test_sequenced_destroy_session_is_exclusive_with_slot_workers() {
    let (fs, gate) = gated_fs(&[("block.txt", 0)], &["block.txt"], &[]).await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let blocked = getattr_compound("destroy-slot", &sessionid, 1, 0, "block.txt");
    write_rpc_record(&mut stream, 3, 1, &blocked).await;
    // Deterministic: the backend is inside the blocked GETATTR.
    gate.wait_entered(1).await;

    let seq_op = encode_sequence(&sessionid, 1, 1);
    let destroy_op = encode_destroy_session(&sessionid);
    let destroy = encode_compound("sequenced-destroy", &[&seq_op, &destroy_op]);
    write_rpc_record(&mut stream, 4, 1, &destroy).await;
    let replied_early = timeout(NOT_YET_WINDOW, read_rpc_record(&mut stream)).await;

    // A second connection observes the session from outside the gate: SEQUENCE
    // succeeds, so the session still exists even though DESTROY_SESSION has
    // been sitting on the connection for a whole `NOT_YET_WINDOW`.
    let mut probe = connect(port).await;
    let probe_compound = encode_compound("probe", &[&encode_sequence(&sessionid, 1, 2)]);
    let mut probe_resp = send_rpc(&mut probe, 10, 1, &probe_compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut probe_resp);
    assert_eq!(accept_stat, 0);
    let (status, _, _) = parse_compound_header(&mut probe_resp);
    assert_eq!(
        status,
        NfsStat4::Ok as u32,
        "the session must outlive every slot worker that is still executing"
    );

    assert!(
        replied_early.is_err(),
        "SEQUENCE; DESTROY_SESSION must not execute while a slot worker holds the shared lane"
    );

    gate.release();

    // Both replies land once the worker drains. They are correlated by XID
    // because the writer publishes them in completion order, not send order.
    let mut seen_slot = false;
    let mut seen_destroy = false;
    for _ in 0..2 {
        let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
            .await
            .expect("both replies arrive once the blocked slot worker drains");
        let (xid, _) = parse_rpc_reply_fields(&mut resp.clone());
        match xid {
            3 => {
                assert_eq!(assert_getattr_ok(&mut resp), 3);
                seen_slot = true;
            }
            4 => {
                assert_eq!(assert_destroy_session_ok(&mut resp), 4);
                seen_destroy = true;
            }
            other => panic!("unexpected reply xid {other}"),
        }
    }
    assert!(seen_slot, "the blocked slot request completed");
    assert!(seen_destroy, "the sequenced DESTROY_SESSION completed");
}
