use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use embednfs_proto::xdr::XdrDecode;
use embednfs_proto::{FATTR4_SIZE, Fattr4, NfsStat4, OP_READ, OP_SEQUENCE, OpaqueAuth};

use super::NOT_YET_WINDOW;
use crate::common::*;

const LARGE_READ_BYTES: usize = 3 * 1024 * 1024;
const OVERSIZED_FRAGMENT: u32 = 3 * 1024 * 1024;

/// Two large replies that complete in the opposite order to their requests are
/// written as two intact, non-interleaved RPC records, each correlated by XID.
/// Origin: RFC 5531 record framing under out-of-order completion on one connection.
/// RFC: RFC 5531 §11; RFC 8881 §2.10.6.1, §18.22.3.
#[tokio::test]
async fn test_inverted_completion_order_writes_intact_records() {
    let (fs, gate) = gated_fs(
        &[
            ("block.bin", LARGE_READ_BYTES),
            ("fast.bin", LARGE_READ_BYTES),
        ],
        &["block.bin"],
        &[],
    )
    .await;
    let port = start_server_with_fs(fs).await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let seq0 = encode_sequence(&sessionid, 1, 0);
    let rootfh_op = encode_putrootfh();
    let read_op = encode_read(0, LARGE_READ_BYTES as u32);
    let getattr_op = encode_getattr(&[FATTR4_SIZE]);
    let blocked = encode_compound(
        "inverted-slow",
        &[
            &seq0,
            &rootfh_op,
            &encode_lookup("block.bin"),
            &getattr_op,
            &read_op,
        ],
    );
    let seq1 = encode_sequence(&sessionid, 1, 1);
    let fast = encode_compound(
        "inverted-fast",
        &[&seq1, &rootfh_op, &encode_lookup("fast.bin"), &read_op],
    );

    // Request order: blocked first, fast second. Completion order is inverted.
    write_rpc_record(&mut stream, 3, 1, &blocked).await;
    gate.wait_entered(1).await;
    write_rpc_record(&mut stream, 4, 1, &fast).await;

    let (mut first, fragments) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("fast reply arrives first");
    assert!(
        fragments > 1,
        "expected a multi-fragment reply, got {fragments}"
    );
    let (xid, accept_stat) = parse_rpc_reply_fields(&mut first);
    assert_eq!(xid, 4, "the second request completed first");
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut first);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 4);
    let (opnum, _) = parse_op_header(&mut first);
    assert_eq!(opnum, OP_SEQUENCE);
    skip_sequence_res(&mut first);
    let _ = parse_op_header(&mut first);
    let _ = parse_op_header(&mut first);
    let (opnum, op_status) = parse_op_header(&mut first);
    assert_eq!(opnum, OP_READ);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    let (eof, data) = parse_read_res(&mut first);
    assert!(eof);
    assert_eq!(data.len(), LARGE_READ_BYTES);
    assert!(data.iter().all(|byte| *byte == 0x5a));

    gate.release();

    let (mut second, fragments) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("blocked reply");
    assert!(fragments > 1);
    let (xid, accept_stat) = parse_rpc_reply_fields(&mut second);
    assert_eq!(xid, 3);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut second);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 5);
    let _ = parse_op_header(&mut second);
    skip_sequence_res(&mut second);
    let _ = parse_op_header(&mut second);
    let _ = parse_op_header(&mut second);
    let _ = parse_op_header(&mut second);
    let _ = Fattr4::decode(&mut second).unwrap();
    let (opnum, op_status) = parse_op_header(&mut second);
    assert_eq!(opnum, OP_READ);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    let (eof, data) = parse_read_res(&mut second);
    assert!(eof);
    assert_eq!(data.len(), LARGE_READ_BYTES);
    assert!(data.iter().all(|byte| *byte == 0x5a));
}

/// A request split across several RPC record fragments is still reassembled and
/// executed exactly once.
/// Origin: RFC 5531 inbound record reassembly, preserved across the reader/worker split.
/// RFC: RFC 5531 §11; RFC 8881 §18.46.
#[tokio::test]
async fn test_multi_fragment_request_is_reassembled() {
    let port = start_server().await;
    let mut stream = connect(port).await;
    let sessionid = setup_session(&mut stream).await;

    let seq_op = encode_sequence(&sessionid, 1, 0);
    let rootfh_op = encode_putrootfh();
    let getattr_op = encode_getattr(&[FATTR4_SIZE]);
    let compound = encode_compound("fragmented", &[&seq_op, &rootfh_op, &getattr_op]);
    let message = encode_rpc_call(7, 1, &compound, &OpaqueAuth::null(), &OpaqueAuth::null());

    let split = message.len() / 2;
    let head = message.slice(..split);
    let tail = message.slice(split..);
    stream
        .write_all(&(head.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&head).await.unwrap();
    stream
        .write_all(&((tail.len() as u32) | 0x8000_0000).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&tail).await.unwrap();
    stream.flush().await.unwrap();

    let (mut resp, _) = timeout(NOT_YET_WINDOW * 20, read_rpc_record(&mut stream))
        .await
        .expect("reply to the fragmented request");
    let (xid, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(xid, 7);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 3);
}

/// A fragment larger than the configured record limit still closes the
/// connection without a reply.
/// Origin: transport limit preserved from the single-threaded connection handler.
/// RFC: RFC 5531 §11.
#[tokio::test]
async fn test_oversized_fragment_closes_connection() {
    let port = start_server().await;
    let mut stream = connect(port).await;

    stream
        .write_all(&OVERSIZED_FRAGMENT.to_be_bytes())
        .await
        .unwrap();
    stream.flush().await.unwrap();

    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("the server must not stall on an oversized fragment")
        .unwrap_or(0);
    assert_eq!(read, 0, "the connection must be closed without a reply");
}
