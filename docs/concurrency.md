# Concurrency and cancellation safety

This note describes how one TCP connection executes several NFSv4.1 forechannel
requests at a time, and what happens when a request cannot finish normally.

## Connection anatomy

A connection is served by three parts (`crates/embednfs/src/server/transport/`):

| Part | Owns | Count |
| --- | --- | --- |
| Record reader (`record.rs`) | read half | one per connection |
| Request workers (`dispatch.rs`) | one RPC record each | up to `max_concurrent_requests` |
| Response writer (`record.rs`) | write half | one per connection |

The reader frames RFC 5531 records, decodes the RPC call header, and spawns one
worker per record. Workers publish complete encoded replies over a bounded
channel; the writer is the only task that writes to the socket.

Consequences:

* **Records never interleave.** Fragmentation happens inside the writer, one
  record at a time, so fragments of two replies can never mix even though the
  replies were produced concurrently.
* **Replies may be reordered.** A reply is written when its worker finishes.
  Clients correlate by RPC XID, as RFC 5531 requires.
* **Nothing is unbounded.** One capacity permit covers a request's whole
  lifecycle: it is acquired *before* the next record is read, travels with the
  encoded reply through the response queue, and is returned only once the writer
  has flushed that reply to the socket. Running workers and queued replies
  therefore draw on a single budget, and a connection holds at most
  `max_concurrent_requests` bodies in total — a peer that stops reading cannot
  make it hold two budgets' worth. Unread records stay in the socket receive
  buffer and TCP flow control pushes back on the client.
  The default limit is `DEFAULT_MAX_CONCURRENT_REQUESTS` (64), which equals the
  advertised forechannel slot count (`fore_chan_attrs.maxrequests`), so a
  conforming client is never throttled below its own slot table.
  `NfsServerBuilder::max_concurrent_requests` adjusts it within
  `1..=MAX_CONCURRENT_REQUESTS_LIMIT`; `1` restores fully serialized handling.

## Lanes

Requests take a per-connection gate before executing. The lane follows from the
**whole** operation array, not just its first operation:

* a minorversion-1 COMPOUND that starts with SEQUENCE and contains no lifecycle
  operation takes the gate **shared**, so different slots run concurrently;
* every other COMPOUND takes it **exclusively**. That covers unsequenced
  control requests (EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION,
  DESTROY_CLIENTID, BIND_CONN_TO_SESSION), requests that are about to be
  rejected for their minor version or missing SEQUENCE, *and* sequenced
  COMPOUNDs that carry a lifecycle operation — `SEQUENCE; DESTROY_SESSION` is a
  legal forechannel request (RFC 8881 §18.37.3), and running it beside other
  slots would let it destroy the session a live worker is about to finalize its
  slot in, turning that finalization into an NFS4ERR_BADSESSION failure that
  loses the executed reply.

Session creation and destruction therefore keep the conservative "nothing else
is running on this connection" behavior they had when requests were handled one
at a time. The gate is a FIFO-fair semaphore, so a waiting control request is
not starved by a stream of slot requests.

## Slot semantics

The slot table, not the worker pool, decides what may execute:

* `prepare_sequence` runs before any filesystem dispatch and marks the slot
  in-progress; the state-manager write lock is released before the COMPOUND is
  executed, so no filesystem call ever runs under it.
* A slot executes at most one request at a time. A retry of the in-progress
  request gets NFS4ERR_DELAY, a *new* sequence id on a busy slot also gets
  NFS4ERR_DELAY (RFC 8881 §2.10.6.1 allows one outstanding request per slot), a
  mismatched retry gets NFS4ERR_SEQ_FALSE_RETRY, and misordered ids keep their
  existing errors.
* `finish_sequence` caches the final encoded `Compound4Res` body **before** the
  worker publishes the reply to the writer. A retry can therefore never observe
  a finished request as still in progress.

## Cancellation and failure

* **Client disconnect or writer failure.** Once a worker is past
  `prepare_sequence` it is never cancelled. It finishes execution, finalizes the
  replay cache entry, and only then tries to publish; a failed publish is
  dropped. The client picks the result up from the replay cache when it retries
  on a new connection. The connection task reclaims its full execution capacity
  before exiting, which waits for every dispatched worker even when the writer
  is the part that died, so cleanup is never orphaned mid-flight.
* **The writer is watched while the next record is read.** A reply for a record
  read after the write half broke could never be delivered, so the reader stops
  at the first observed writer failure: no *new* record is dispatched, and no
  side effects are committed for a request whose outcome the client can never
  learn. Records already dispatched still run to completion and finalize.
* **Panic or task cancellation between prepare and finish.** A worker that dies
  in that window would otherwise leave the slot in-progress forever. A drop
  guard (`transport/replay.rs`) instead finalizes the slot with a replayable
  NFS4ERR_SERVERFAULT COMPOUND. `finish` keeps the slot token until the reply is
  actually cached — awaiting the state lock is its only cancellation point — so
  a worker cancelled there, or one whose finalization fails, still falls back to
  the guard instead of leaving the slot in progress with no owner. The slot is *not* silently made reusable,
  because the dead request may already have performed side effects: the sequence
  id stays consumed, an identical retry replays the fault rather than executing
  again, and a different request on that slot still gets
  NFS4ERR_SEQ_FALSE_RETRY. The guard finalizes synchronously when the state lock
  is free and otherwise hands the work to the runtime. Only if it runs with no
  runtime at all (process teardown) does it log an error and leave the slot to
  session destruction or lease expiry.

## Tests

`tests/forechannel_concurrency.rs` covers slot scheduling, the control lane, the
concurrency bound, replay across disconnects and worker faults, and record
framing under inverted completion order.

`src/server/transport/tests.rs` covers the failure points a socket cannot
express exactly: cancelling `finish` while it waits for the state lock, a write
failure observed while a further record is already readable, and a write half
that freezes without ever failing, which pins the capacity budget in place.
