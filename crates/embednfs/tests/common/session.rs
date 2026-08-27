use tokio::net::TcpStream;

use embednfs_proto::{NfsStat4, OP_CREATE_SESSION, OP_EXCHANGE_ID};

use super::encode::{
    encode_compound, encode_create_session, encode_create_session_with_fore_limits,
    encode_exchange_id, encode_exchange_id_with_name,
};
use super::parse::{
    parse_compound_header, parse_create_session_res, parse_op_header, parse_rpc_reply_fields,
    skip_exchange_id_res,
};
use super::transport::send_rpc;

pub async fn setup_session(stream: &mut TcpStream) -> [u8; 16] {
    let exchange_id_op = encode_exchange_id();
    let compound = encode_compound("exchange", &[&exchange_id_op]);
    let mut resp = send_rpc(stream, 1, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_EXCHANGE_ID);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    let (clientid, sequenceid) = skip_exchange_id_res(&mut resp);

    let create_session_op = encode_create_session(clientid, sequenceid);
    let compound = encode_compound("create-session", &[&create_session_op]);
    let mut resp = send_rpc(stream, 2, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_CREATE_SESSION);
    assert_eq!(op_status, NfsStat4::Ok as u32);

    parse_create_session_res(&mut resp)
}

pub async fn setup_session_full(stream: &mut TcpStream) -> ([u8; 16], u64) {
    let exchange_id_op = encode_exchange_id();
    let compound = encode_compound("exchange", &[&exchange_id_op]);
    let mut resp = send_rpc(stream, 1, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_EXCHANGE_ID);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    let (clientid, sequenceid) = skip_exchange_id_res(&mut resp);

    let create_session_op = encode_create_session(clientid, sequenceid);
    let compound = encode_compound("create-session", &[&create_session_op]);
    let mut resp = send_rpc(stream, 2, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_CREATE_SESSION);
    assert_eq!(op_status, NfsStat4::Ok as u32);

    let sessionid = parse_create_session_res(&mut resp);
    (sessionid, clientid)
}

/// Runs EXCHANGE_ID alone and returns the client id with the sequence id its
/// first CREATE_SESSION must use.
pub async fn exchange_id_only(stream: &mut TcpStream, xid: u32, owner: &[u8]) -> (u64, u32) {
    let exchange_id_op = encode_exchange_id_with_name(owner);
    let compound = encode_compound("exchange", &[&exchange_id_op]);
    let mut resp = send_rpc(stream, xid, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_EXCHANGE_ID);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    skip_exchange_id_res(&mut resp)
}

/// Creates one session for an already-exchanged client, negotiating the given
/// forechannel reply limits. A client may hold several sessions at once; each
/// CREATE_SESSION consumes the client's next sequence id.
pub async fn create_session_with_fore_limits(
    stream: &mut TcpStream,
    xid: u32,
    clientid: u64,
    seq: u32,
    maxresponsesize: u32,
    maxresponsesize_cached: u32,
) -> [u8; 16] {
    let create_session_op = encode_create_session_with_fore_limits(
        clientid,
        seq,
        maxresponsesize,
        maxresponsesize_cached,
    );
    let compound = encode_compound("create-session", &[&create_session_op]);
    let mut resp = send_rpc(stream, xid, 1, &compound).await;
    let (_, accept_stat) = parse_rpc_reply_fields(&mut resp);
    assert_eq!(accept_stat, 0);
    let (status, _, num_results) = parse_compound_header(&mut resp);
    assert_eq!(status, NfsStat4::Ok as u32);
    assert_eq!(num_results, 1);
    let (opnum, op_status) = parse_op_header(&mut resp);
    assert_eq!(opnum, OP_CREATE_SESSION);
    assert_eq!(op_status, NfsStat4::Ok as u32);
    parse_create_session_res(&mut resp)
}

/// Exchanges a fresh client id and creates one session with the given
/// forechannel reply limits.
pub async fn setup_session_with_fore_limits(
    stream: &mut TcpStream,
    owner: &[u8],
    maxresponsesize: u32,
    maxresponsesize_cached: u32,
) -> [u8; 16] {
    let (clientid, seq) = exchange_id_only(stream, 1, owner).await;
    create_session_with_fore_limits(
        stream,
        2,
        clientid,
        seq,
        maxresponsesize,
        maxresponsesize_cached,
    )
    .await
}
