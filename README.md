# Homework 3 - Reliable protocol over UDP

A small reliable byte-stream protocol on top of UDP, similar in spirit to TCP.
The implementation lives in `lib/libsend.cpp` (client/sender side) and
`lib/librecv.cpp` (server/receiver side). `lib/libcommon.cpp` holds the shared
helper that waits for either incoming data or a timer expiry.

## Files
- `lib/libsend.cpp` - sender API (`setup_connection`, `send_data`, `init_sender`)
- `lib/librecv.cpp` - receiver API (`wait4connect`, `recv_data`, `init_receiver`)
- `lib/libcommon.cpp` - `recv_message_or_timeout` used by both handler threads
- `lib/include/protocol.h` - on-wire headers and segment type constants
- `lib/include/lib.h` - shared `struct connection` and the public API

## Segment format
Two header kinds, exactly as required by the assignment:

Data segment: `protocol_id | conn_id | type | seq_num | len | payload`
Control segment: `protocol_id | conn_id | type | ack_num | recv_window`

`type` is one of `POLI_TYPE_DATA`, `POLI_TYPE_ACK`, `POLI_TYPE_SYN`,
`POLI_TYPE_SYNACK`.

## Connection setup (three-way handshake)
1. Server binds an "accept" UDP socket on port 8032 (done lazily inside the
   first `wait4connect`).
2. Client sends a `SYN` to that port.
3. Server opens a fresh socket on a random local port, sends back a `SYN-ACK`
   whose payload contains that port (network byte order).
4. Client updates its destination port to the one from the payload and sends
   the final `ACK`. All further traffic uses the per-connection port.

Both sides use a short receive timeout during the handshake and retransmit
the SYN / SYN-ACK if nothing arrives. The server also treats an incoming
`DATA` segment on the new socket as an implicit confirmation of the ACK,
so a lost final ACK is not fatal.

## Reliable transfer
- Each data segment carries a 16-bit sequence number, incremented per segment.
- The receiver replies with a **cumulative ACK** carrying the next expected
  sequence number plus the current free space in its application buffer
  (used as `recv_window`).
- The sender keeps an "inflight" deque of unacked segments. When an ACK
  arrives, every segment with `seq < ack_num` is dropped from the deque.
- Out-of-order segments at the receiver are buffered in a `seq -> payload`
  map. When the in-order `expected_seq` arrives, contiguous buffered
  segments are flushed into the user's recv buffer (selective receive,
  cumulative ACK).
- One timer per connection (100 ms on the sender side) drives retransmits:
  on each tick, every still-unacked segment is sent again. The receiver
  has a timer too, but it does nothing on fire.

## Window
The sender allows up to `MAX_INFLIGHT = 16` segments of 512 bytes (8 KB)
in flight. That fits comfortably in the 9 KB buffer advertised by the
server in `init_receiver`. `send_data` keeps calling `sendto` while the
window has room and the request is not satisfied, sleeping briefly to wait
for ACKs to free space - this avoids the 500 ms backoff the test client
does whenever `send_data` returns less than requested.

## Threads
- One handler thread per side, started by `init_sender` / `init_receiver`.
- The application thread runs `send_data` / `recv_data`.
- A per-connection mutex (`con_lock`) guards each connection's state so the
  two threads can safely cooperate.

## Build / run
```
make
./server                 # one client
./server N               # N simultaneous clients
./client path/to/file
```
