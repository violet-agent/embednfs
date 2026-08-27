//! Bounds the reply one COMPOUND may produce.
//!
//! CREATE_SESSION negotiates `ca_maxresponsesize`: "the maximum size of a
//! COMPOUND ... reply that the requester will accept from the replier including
//! RPC headers ... but excludes any RPC transport framing headers"
//! (RFC 8881 §18.36.3). Without it, `READ.count` and `READDIR.maxcount` are
//! client-chosen 32-bit sizes that the server would hand straight to the
//! filesystem: one request could ask a backend for four gigabytes.
//!
//! A budget is created per COMPOUND from the negotiated limit, minus the exact
//! encoding of the reply envelope, and is then *consumed* by each result as it
//! is produced. Consuming rather than clamping per operation is what makes two
//! READs in one COMPOUND add up to the limit instead of to twice the limit, and
//! what makes the operations in front of a READ count against it.
//!
//! # Why the accounting is exact
//!
//! A result that carries no bulk payload is measured by encoding it with the
//! very encoder that will write it to the wire, so the budget cannot drift from
//! the encoding the way a hand-written length table would. The three results
//! that can be large — READ, READDIR, READLINK — are measured arithmetically
//! instead, because copying a megabyte twice to learn its length is exactly the
//! cost this module exists to avoid; `tests::test_payload_result_len_matches_the_encoder`
//! pins those formulas to the encoder.
//!
//! Measuring is skipped entirely for a COMPOUND that contains no payload
//! operation ([`compound_has_payload_op`]): there is then nothing whose size
//! depends on the budget, so the common GETATTR/LOOKUP/WRITE path pays nothing.
//!
//! Returning less than was asked for is always legal: RFC 8881 §18.22.3 lets a
//! server end a READ before the requested count, and §18.23.3 makes `maxcount`
//! a ceiling on the `READDIR4resok` the server chooses to return.

use bytes::BytesMut;
use embednfs_proto::xdr::{XdrEncode, xdr_pad};
use embednfs_proto::{NfsArgop4, NfsResop4};

use crate::session::{ForeChannelLimits, MAX_RESPONSE_SIZE};

use super::{readdir_resok_len, xdr_opaque_len};

/// XID, message type, reply status, an AUTH_NONE verifier (flavor plus a
/// zero-length body), and the accept status.
const RPC_ACCEPTED_REPLY_LEN: usize = 4 + 4 + 4 + (4 + 4) + 4;

/// `COMPOUND4res` status, the tag's length prefix, and the result array count.
/// The tag's own bytes are added per COMPOUND.
const COMPOUND_RES_FIXED_LEN: usize = 4 + 4 + 4;

/// Opcode, status, and the eof flag that precede a READ's opaque data.
const READ_RES_HEADER_LEN: usize = 4 + 4 + 4;

/// Opcode and status that precede a `READDIR4resok`.
const READDIR_RES_HEADER_LEN: usize = 4 + 4;

/// Opcode and status that precede a READLINK's opaque link text.
const READLINK_RES_HEADER_LEN: usize = 4 + 4;

/// The length prefix of an XDR opaque plus its worst-case padding.
const XDR_OPAQUE_HEADER_LEN: usize = 4 + 3;

/// Whether any operation in `argarray` produces a result whose size the budget
/// controls. Only those COMPOUNDs need their results measured.
pub(super) fn compound_has_payload_op(argarray: &[NfsArgop4]) -> bool {
    argarray.iter().any(|op| {
        matches!(
            op,
            NfsArgop4::Read(_) | NfsArgop4::Readdir(_) | NfsArgop4::Readlink
        )
    })
}

/// Room left in the COMPOUND reply being built.
pub(super) struct ResponseBudget {
    remaining: usize,
    /// Scratch buffer for measuring one result at a time. `None` when the
    /// COMPOUND has no payload operation, in which case no result is measured
    /// and no limit is ever consulted.
    scratch: Option<BytesMut>,
}

impl ResponseBudget {
    /// Budget for a COMPOUND that runs under a session with these `limits`.
    pub(super) fn for_session(limits: ForeChannelLimits, tag: &str, measured: bool) -> Self {
        Self::new(limits.max_response_size, tag, measured)
    }

    /// Budget for a COMPOUND that runs without a session.
    ///
    /// Only the operations RFC 8881 §18 allows outside a session reach this
    /// path (EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION, DESTROY_CLIENTID,
    /// BIND_CONN_TO_SESSION), and none of them carries bulk payload, so no
    /// other session's negotiated limit is borrowed for them: the server's own
    /// hard cap applies instead.
    pub(super) fn unsequenced(tag: &str, measured: bool) -> Self {
        Self::new(MAX_RESPONSE_SIZE, tag, measured)
    }

    fn new(max_response_size: u32, tag: &str, measured: bool) -> Self {
        // `COMPOUND_RES_FIXED_LEN` already carries the tag's length prefix, so
        // only the tag's bytes and their padding are added here.
        let envelope =
            RPC_ACCEPTED_REPLY_LEN + COMPOUND_RES_FIXED_LEN + tag.len() + xdr_pad(tag.len());
        Self {
            remaining: (max_response_size as usize).saturating_sub(envelope),
            scratch: measured.then(|| BytesMut::with_capacity(256)),
        }
    }

    /// Largest `READ.count` whose result still fits the remaining budget.
    pub(super) fn read_limit(&self) -> u32 {
        let payload = self
            .remaining
            .saturating_sub(READ_RES_HEADER_LEN + XDR_OPAQUE_HEADER_LEN);
        u32::try_from(payload).unwrap_or(u32::MAX)
    }

    /// Largest `READDIR.maxcount` whose result still fits the remaining budget.
    ///
    /// `maxcount` bounds the `READDIR4resok` structure only (RFC 8881
    /// §18.23.3), which is what `readdir_resok_len` measures, so only the
    /// opcode and status are deducted here.
    pub(super) fn readdir_limit(&self) -> u32 {
        let payload = self.remaining.saturating_sub(READDIR_RES_HEADER_LEN);
        u32::try_from(payload).unwrap_or(u32::MAX)
    }

    /// Deducts exactly what `res` will occupy in the encoded reply.
    pub(super) fn consume(&mut self, res: &NfsResop4) {
        let Some(scratch) = self.scratch.as_mut() else {
            return;
        };
        let len = match payload_result_len(res) {
            Some(len) => len,
            None => {
                scratch.clear();
                res.encode(scratch);
                scratch.len()
            }
        };
        self.remaining = self.remaining.saturating_sub(len);
    }
}

/// Encoded length of a result that carries bulk payload, or `None` for results
/// that are cheap enough to measure by encoding them.
fn payload_result_len(res: &NfsResop4) -> Option<usize> {
    match res {
        NfsResop4::Read(_, Some(read)) => {
            Some(READ_RES_HEADER_LEN + xdr_opaque_len(read.data.len()))
        }
        NfsResop4::Readdir(_, Some(readdir)) => {
            Some(READDIR_RES_HEADER_LEN + readdir_resok_len(&readdir.entries, readdir.eof))
        }
        NfsResop4::Readlink(_, Some(target)) => {
            Some(READLINK_RES_HEADER_LEN + xdr_opaque_len(target.len()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code asserts by panicking; a failed unwrap is a failed test"
    )]

    use bytes::Bytes;
    use embednfs_proto::{Bitmap4, Entry4, Fattr4, NfsStat4, ReadRes4, ReaddirRes4, SequenceRes4};

    use super::*;

    /// The exact number of bytes `res` adds to an encoded `COMPOUND4res`.
    fn encoded_len(res: &NfsResop4) -> usize {
        let mut buf = BytesMut::new();
        res.encode(&mut buf);
        buf.len()
    }

    fn read_res(len: usize) -> NfsResop4 {
        NfsResop4::Read(
            NfsStat4::Ok,
            Some(ReadRes4 {
                eof: false,
                data: Bytes::from(vec![0x5a; len]),
            }),
        )
    }

    fn readdir_res(names: &[&str]) -> NfsResop4 {
        NfsResop4::Readdir(
            NfsStat4::Ok,
            Some(ReaddirRes4 {
                cookieverf: [0u8; 8],
                entries: names
                    .iter()
                    .enumerate()
                    .map(|(idx, name)| Entry4 {
                        cookie: idx as u64 + 3,
                        name: (*name).to_string(),
                        attrs: Fattr4 {
                            attrmask: Bitmap4::new(),
                            attr_vals: Bytes::new(),
                        },
                    })
                    .collect(),
                eof: true,
            }),
        )
    }

    /// The arithmetic used for the results the budget refuses to re-encode must
    /// agree with the encoder, or every clamp derived from it is wrong. Lengths
    /// that are not a multiple of four cover the XDR padding.
    #[test]
    fn test_payload_result_len_matches_the_encoder() {
        for len in [0usize, 1, 3, 4, 5, 4096] {
            let res = read_res(len);
            assert_eq!(
                payload_result_len(&res),
                Some(encoded_len(&res)),
                "READ {len}"
            );
        }

        for names in [&[][..], &["a"][..], &["abc", "defg", "hi"][..]] {
            let res = readdir_res(names);
            assert_eq!(
                payload_result_len(&res),
                Some(encoded_len(&res)),
                "READDIR {names:?}"
            );
        }

        for target in ["", "a", "abc", "abcd", "some/longer/path"] {
            let res = NfsResop4::Readlink(NfsStat4::Ok, Some(target.to_string()));
            assert_eq!(
                payload_result_len(&res),
                Some(encoded_len(&res)),
                "READLINK {target:?}"
            );
        }
    }

    const TEST_LIMITS: ForeChannelLimits = ForeChannelLimits {
        max_response_size: 4096,
        max_response_size_cached: 4096,
    };

    fn sequence_res() -> NfsResop4 {
        NfsResop4::Sequence(
            NfsStat4::Ok,
            Some(SequenceRes4 {
                sessionid: [0u8; 16],
                sequenceid: 1,
                slotid: 0,
                highest_slotid: 7,
                target_highest_slotid: 7,
                status_flags: 0,
            }),
        )
    }

    /// Two READs in one COMPOUND share a single budget, and the operations in
    /// front of them are charged against it too, so the whole reply stays
    /// inside the negotiated size.
    #[test]
    fn test_results_share_one_budget() {
        let tag = "two-reads";
        let mut budget = ResponseBudget::for_session(TEST_LIMITS, tag, true);

        let sequence = sequence_res();
        let putfh = NfsResop4::Putrootfh(NfsStat4::Ok);
        budget.consume(&sequence);
        budget.consume(&putfh);

        // What one READ could have taken had it been the only one. The first
        // READ asks for less than that, the way a client asking for a fixed
        // block size does.
        let whole = budget.read_limit() as usize;
        let first = whole / 2;
        let first_res = read_res(first);
        budget.consume(&first_res);

        let second = budget.read_limit() as usize;
        assert!(second > 0, "the second READ still has room");
        assert!(
            second <= whole - first,
            "the first READ's own result must be charged against the shared \
             budget: {second} bytes left of {whole} after reading {first}"
        );

        let second_res = read_res(second);
        budget.consume(&second_res);
        let reply = RPC_ACCEPTED_REPLY_LEN
            + COMPOUND_RES_FIXED_LEN
            + tag.len()
            + xdr_pad(tag.len())
            + encoded_len(&sequence)
            + encoded_len(&putfh)
            + encoded_len(&first_res)
            + encoded_len(&second_res);
        assert!(
            reply <= TEST_LIMITS.max_response_size as usize,
            "reply of {reply} bytes exceeds the negotiated {}",
            TEST_LIMITS.max_response_size
        );
        // The clamp is not merely safe but tight: filling both READs to their
        // limits leaves nothing behind but the padding `read_limit` reserves
        // for an opaque of unknown length.
        assert!(
            TEST_LIMITS.max_response_size as usize - reply < 4,
            "reply of {reply} bytes wastes more than XDR padding of the \
             negotiated {}",
            TEST_LIMITS.max_response_size
        );
    }

    /// A first READ that takes everything the budget allows leaves nothing for
    /// a second one, which is the same sharing seen from the other end: the
    /// budget is consumed by the result, not restored per operation.
    ///
    /// A zero-length second result is legal — RFC 8881 §18.22.3 lets a READ
    /// return fewer bytes than asked for — and the client reads the remainder
    /// with its next COMPOUND.
    #[test]
    fn test_a_maximal_read_leaves_nothing_for_the_next_one() {
        let mut budget = ResponseBudget::for_session(TEST_LIMITS, "greedy-read", true);
        budget.consume(&sequence_res());

        let first = budget.read_limit() as usize;
        assert!(first > 0, "the first READ has the whole budget");
        budget.consume(&read_res(first));

        assert_eq!(budget.read_limit(), 0, "the budget is spent, not renewed");
        assert_eq!(budget.readdir_limit(), 0);
    }
}
