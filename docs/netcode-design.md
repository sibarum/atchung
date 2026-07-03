# elektro-Q netcode: design proposal

**Status:** proposal for review — not yet implemented. Supersedes the one-line "UDP transport"
roadmap item with a real plan for a general-purpose real-time netcode framework (authoritative-server
games, P2P games, voice/video, and ordinary reliable messaging) that stays true to elektro-Q's ethos:
reflection-free, GraalVM-native-clean, transport-behind-an-SPI.

---

## 1. The problem UDP forces

elektro-Q today assumes **reliable, ordered** delivery: request/reply correlation, per-peer actor
ordering, "a reply always comes back." TCP gives that for free. UDP gives none of it — datagrams can
be **lost, reordered, duplicated**, and are **bounded by the path MTU**. But raw UDP is also the only
way to get the low, jitter-free latency real-time apps need, because TCP's in-order byte stream causes
**head-of-line blocking**: one lost segment stalls everything behind it, which is fatal for a 60 Hz
game or a voice call.

So the framework's job is not "make UDP reliable" (that's just TCP). It's to offer a **spectrum of
delivery guarantees, chosen per message**, and to layer *only the reliability a given stream actually
needs* on top of an unreliable datagram base — the design shared by ENet, Valve's
GameNetworkingSockets, QUIC, and netcode.io.

## 2. Design principles

1. **Delivery mode is a first-class, per-message choice** — not a property of the whole connection.
2. **Independent channels, no cross-stream head-of-line blocking** (the QUIC insight): a dropped
   reliable event must never stall unrelated position updates.
3. **Layered, each layer optional** — you can use raw unreliable datagrams (L0) alone, or opt into
   reliability, congestion control, crypto, and media features à la carte.
4. **Reuse elektro-Q's seam.** The netcode packet protocol lives *below* the message envelope, behind
   the existing `Transport` SPI. Message framing (typeId/schema/correlation) is unchanged; the
   `FRAGMENT` and `COMPRESSED` flags already reserved in `Flags` were left for exactly this.
5. **Reflection-free & native-clean.** Packet codecs are hand-written against `WireReader`/`WireWriter`,
   like every other elektro-Q codec.
6. **Testable by construction.** A built-in network simulator (latency/jitter/loss/reorder/dup) and
   exposed metrics (RTT, loss %, bandwidth, retransmits) are part of the framework, not an afterthought
   — and the metrics feed straight into the existing [debug port](../README.md).

## 3. Where it sits

```
  application  ─────────────────────────────────────────────
      │  Action/Actor, DynValue, request/reply   (unchanged)
  message envelope (24-byte header: typeId, schema, corr, flags, len)
      │
  ┌── netcode layer (NEW) ────────────────────────────────┐
  │  L6 media profile   (FEC, jitter buffer, deadlines)   │
  │  L5 congestion control + pacing                       │
  │  L4 fragmentation / reassembly                        │
  │  L3 channels + delivery modes                         │
  │  L2 packet layer: sequence #, ack bitfield, RTT/loss  │
  │  L1 connection: handshake, conn-id, keepalive, MTU    │
  └───────────────────────────────────────────────────────┘
      │  Transport SPI: send(PeerId, ByteBuffer) / onFrame
  L0 UdpTransport   (raw datagrams, non-blocking / vthreads)
```

The **message envelope** is per-*message* framing (what type is this, who's it a reply to). The
**packet header** (L2) is per-*datagram* framing (sequence, acks) and is independent — the same
separation the README already draws between framing and transport. One datagram may pack several small
messages; one large message may span several datagram fragments.

## 4. The layers

### L0 — `UdpTransport` (raw datagrams)
Non-blocking UDP over NIO `DatagramChannel` (single selector thread) or a virtual-thread blocking
receive loop, mirroring `TcpTransport`'s shape. Maps `PeerId` ↔ `InetSocketAddress`. Implements the
existing `Transport` SPI directly, so **fire-and-forget datagrams work immediately** — this alone is
"cross-network unreliable-unordered," the cheapest useful thing, and it exercises the transport seam a
third way (after TCP and local). No reliability, ≤ MTU payloads.

### L1 — Connection & session
UDP is connectionless; games and calls need a connection: a **handshake** (SYN/challenge/accept),
a **connection id** independent of the 4-tuple so a session survives NAT rebinding or a Wi-Fi→cellular
switch (QUIC's key idea), **keepalive/heartbeat** with idle timeout, and **path MTU discovery** (probe
down from ~1200 bytes; DPLPMTUD-style). Optionally a **challenge-response / connect token** to prevent
IP spoofing and reflection/amplification abuse (netcode.io's model).

### L2 — Packet layer: sequencing + acks (the heart)
Every outgoing datagram carries a compact header:

```
protocolId(4) | connId(4) | sequence(2, wrapping) | ack(2) | ackBits(4) | channelId(1) | flags(1)
```

`ack` = the highest sequence received from the peer; `ackBits` = a 32-bit mask of the 32 packets before
that. This single scheme (Fiedler / GameNetworkingSockets) gives, for free: **RTT estimation** (time
between sending seq N and seeing it acked), **loss detection** (a bit never set), and the substrate for
reliability — a reliable message is retransmitted until the packet(s) that carried it are acked. No
per-message ack round-trips, no TCP-style per-byte sequence.

### L3 — Channels & delivery modes
A connection multiplexes independent **channels**, each with a mode. Loss/ordering on one channel never
affects another (no cross-channel HOL blocking).

| Mode | Delivered? | Ordered? | Drops stale? | Typical use |
|---|---|---|---|---|
| **Unreliable** | best-effort | no | no | metrics/telemetry you'll resend anyway |
| **Unreliable-sequenced** | best-effort | newest-only | **yes** | entity position, animation, cursor — only the latest matters |
| **Reliable-unordered** | **yes** | no | no | discrete events (item pickup, door opened) |
| **Reliable-ordered** | **yes** | **yes** (per channel) | no | RPC, chat, handshakes — HOL blocking *within this channel only* |
| **Media** (§L6) | within deadline | by timestamp | past-deadline | voice/video frames |

`request/reply` maps onto a reliable-ordered channel with the existing `correlationId`. The current
`Conduit` behaviour = one reliable-ordered channel, so today's code is the default case unchanged.

### L4 — Fragmentation & reassembly
Messages larger than the path MTU are split into fragments tagged `(messageId, fragIndex, fragCount)`
using the reserved `FRAGMENT` flag. Reliable fragments ride the reliability layer (each retransmitted
until acked); an unreliable message is dropped whole if any fragment is missing past a reassembly
timeout. Keeps large reliable payloads (a level snapshot) working without bloating small-packet paths.

### L5 — Congestion control & pacing
A general-purpose framework **must** be a good internet citizen — mandatory once media is in play.
Pluggable controller reading RTT + loss from L2: at minimum a good/bad-mode bandwidth throttle
(Fiedler) for games; ideally a real controller (BBR-like or a CCC variant) for media. **Pacing**
spreads a send budget over time instead of bursting. Exposes a bytes/sec budget the channels draw from,
reliable traffic prioritised over unreliable under pressure.

### L6 — Media profile (voice/video)
Real-time media diverges from game state and needs its own toolkit on top of L2–L3:
- **Deadline-based partial reliability.** A video frame past its play-out deadline is useless —
  never retransmit it; skip. Modeled as a channel mode "reliable-until-deadline."
- **FEC (forward error correction).** Send parity (XOR for the simple case, Reed-Solomon / FlexFEC for
  better) so isolated losses recover with **no round-trip** — essential for voice where a retransmit RTT
  is already too late. (Mirrors WebRTC ULPFEC/FlexFEC and Opus in-band FEC.)
- **Adaptive jitter buffer.** Receiver-side buffer that smooths arrival jitter before playout; the
  framework ships a reusable one (it's app/receiver-side, not transport).
- **Media timestamps.** Packets carry capture timestamps (RTP-style) for playout scheduling and A/V
  sync.
- **NACK + keyframe request.** For video, NACK lost packets *if* they can still arrive before deadline;
  on unrecoverable loss, request a keyframe (PLI-style) rather than retransmit forever.

Net: the media profile = unreliable-sequenced base + FEC + bounded NACK + jitter buffer + timestamps —
essentially a lean RTP/WebRTC-media transport riding the same L0–L2 as the game path.

## 4b. Connections, broadcast, and addressing

The framework is multi-peer from the base `Transport`/`Conduit` model up, and the netcode layers
preserve that per peer:

- **Many connections per endpoint.** One `ReliableTransport` holds N independent sessions keyed by
  `PeerId` — each with its own `ReliableEndpoint` (sequence space, RTT, loss) and its own
  channel send/receive state (per-channel sequences, reliable queues, ordering buffers). A server
  on `listening` discovers and handshakes each client separately; `conduit.peers()` reflects the
  live set.
- **Broadcast vs. specific destination.** `emit(msg)` → `PeerId.BROADCAST` fans out to every
  established peer; `emit(msg, peer)` and `request(msg, peer, …)` target one. This is
  **application-level fan-out (N independent unicast sends), not IP multicast** — and a *reliable*
  broadcast is therefore **N independent reliable streams** (the payload is queued, sequenced,
  retransmitted, and ordered separately per peer, because peers have independent loss). Correct, and
  the right model for per-client state broadcast, but its cost scales with peer count.
- **The gap — initiating multiple *outbound* connections.** The current `Role` split is asymmetric:
  a server accepts many, a client dials exactly one upstream. A node that wants to *dial several
  specific peers* (a P2P mesh, or a client of multiple servers) isn't cleanly supported yet. The fix
  is a **symmetric peer model**: `UdpTransport.connectTo(address) → PeerId` (add a peer + initiate)
  plus a role that both listens and dials on demand, instead of binary client/server. This is P2P
  territory and travels with the deferred NAT-traversal track; the authoritative-server model needs
  none of it.

## 5. Cross-cutting concerns

- **Encryption/auth (built-in, optional).** Per-packet AEAD (X25519 handshake + ChaCha20-Poly1305,
  libsodium/netcode.io style) or DTLS. Anti-replay via the sequence number + nonce. For a
  general-purpose framework this should be on by default for internet traffic.
- **NAT traversal.** ICE-lite + STUN for address discovery and UDP hole-punching, with a TURN relay
  fallback when both peers are cone-blocked (RFC 5389/8445/5766). Needed for P2P (voice/video, P2P
  games); irrelevant for authoritative-server. Deferred to its own module.
- **Network simulator.** A `SimTransport` wrapper injecting latency, jitter, loss, reorder, and
  duplication — netcode is untestable without it.
- **Metrics & the debug port.** RTT, loss %, up/down bandwidth, retransmit rate, per-channel queue
  depth — surfaced through the same telemetry channel the editor debug port already uses.
- **Native-image.** Hand-written packet codecs, no reflection; a `DatagramChannel` selector needs no
  reachability metadata.

## 6. Module structure

- **`elektroq-netcode`** (new) — `UdpTransport` (L0), the reliable endpoint / packet protocol (L1–L2),
  channels (L3), fragmentation (L4), congestion control (L5), the network simulator, and a
  channel-aware conduit. Depends on `elektroq-core` only.
- **`elektroq-netcode-media`** (new, optional) — the L6 media profile: FEC, jitter buffer, deadline
  channel, NACK/keyframe. Depends on `elektroq-netcode`.
- **`elektroq-netcode-crypto`** *(or folded into netcode)* — the AEAD handshake + per-packet cipher.
- **`elektroq-netcode-p2p`** (later) — STUN/ICE/TURN.

## 7. API sketch (how it looks upward)

Delivery mode is a send-side argument; the triad is otherwise unchanged. Actors subscribe by type as
today (the sequenced-drop is applied on the receive path before dispatch).

```java
// entity state: newest-only, no retransmit, no HOL blocking
Action<EntityState> state = conduit.action(EntityStateCodec.TYPE, Delivery.UNRELIABLE_SEQUENCED);
state.emit(new EntityState(tick, pos, vel), peer);

// discrete gameplay event: guaranteed, order-independent
conduit.action(ItemPickedUpCodec.TYPE, Delivery.RELIABLE_UNORDERED).emit(evt, peer);

// RPC: reliable-ordered request/reply (today's default, explicit)
conduit.action(FireCodec.TYPE, Delivery.RELIABLE_ORDERED).request(fire, peer, FireAckCodec.TYPE);

// voice: FEC + jitter buffer + deadline, timestamps for playout
MediaChannel voice = conduit.mediaChannel(MediaProfile.VOICE);
voice.send(opusFrame, captureTimestamp);
voice.onFrame((bytes, ts) -> playout(bytes, ts));
```

For Pontif, this extends [`pontif.net`](../../pontif-framework/pontif-builtin-net): `connect`/`listen`
gain an optional delivery mode, and a `mediaChannel` builtin surfaces the media profile — the same
program spanning thread → process → LAN → internet, now with a latency/reliability knob.

## 7b. Rooms & pub/sub — a socket.io-style surface

The reference point for the app-facing API is socket.io's broadcast/subscription model. Most of it
maps onto primitives we already have; the gap is **rooms** and **broadcast-except-sender**.

| socket.io | elektro-Q | status |
|---|---|---|
| server as fan-out hub | `ReliableTransport`/`Conduit` with N sessions | have |
| `io.to(id).emit` / `io.emit` | `emit(msg, peer)` / `emit(msg)` | have |
| `emit(ev,data,ack)` | `request(msg, peer, replyType)` | have |
| `socket.on("ev")` | typed `subscribe(MessageType, Actor)`; Pontif routes by type name | have (typed) |
| rooms: `join`/`leave`, `io.to("room").emit` | — | **build** |
| `socket.broadcast.emit` (all but sender), `to().to()` union, `except()` | — | **build** |

**Reframe.** socket.io is reliable-ordered-only over TCP/WebSocket. elektro-Q already has the hub and
adds the thing socket.io lacks — per-message delivery modes — so this layer gives socket.io's
ergonomics *plus* a per-emit guarantee, and, being pure membership + fan-out over the `Conduit` API,
it is **transport-agnostic** (works over TCP and the in-VM transport, not only UDP).

Proposed `Rooms` component on a server-side conduit:

```java
Rooms rooms = new Rooms(conduit);
rooms.join(peer, "lobby");                        // server-driven, like socket.join
rooms.to("lobby").emit(type, msg);                // io.to("lobby").emit
rooms.to("lobby").except(sender).emit(type, msg); // socket.to("lobby").emit
rooms.all().except(sender).emit(type, msg);       // socket.broadcast.emit
rooms.to("a").to("b").emit(type, msg);            // union, deduped
rooms.to("game-42").emit(type, msg, UNRELIABLE_SEQUENCED); // + a delivery mode (netcode only)
```

Membership is server-driven (the app calls `join`/`leave`), exactly as socket.io's server calls
`socket.join`; an optional built-in join/leave request lets clients ask. The subscription side stays
elektro-Q's typed `subscribe` (the `on("event")` analog), already string-named at the Pontif layer.

Incremental path: **socket.io parity first** (rooms + broadcast variants, reliable-ordered — exactly
socket.io's semantics), then thread the delivery-mode argument through for the netcode superset. This
layer is transport-agnostic, so it could live in its own small module (`elektroq-hub`) usable over
any transport, rather than inside the netcode module.

## 8. Phasing (incremental — each phase is independently useful)

1. **L0 `UdpTransport` + simulator + metrics.** Raw unreliable datagrams behind the SPI. Usable now for
   unreliable-unordered; proves the seam.
2. **L1–L2**: connection, sequence/ack, RTT/loss, keepalive/timeout. (`ReliableEndpoint`.)
3. **L3–L4**: channels + delivery modes + fragmentation. Channel-aware conduit API.
4. **L5**: congestion control + pacing.
5. **Crypto**: AEAD handshake, anti-replay.
6. **L6 media profile**: FEC, jitter buffer, deadline reliability, NACK/keyframe.
7. **P2P**: STUN/ICE/TURN.

## 9. Prior art this borrows from

ENet (channels + selective reliability over UDP) · Valve GameNetworkingSockets (modern reference for a
game netcode lib: SNP, connection layer) · gafferongames / Glenn Fiedler (sequence+ack-bitfield
reliability, fragmentation, congestion) · QUIC (connection IDs, independent streams, no cross-stream
HOL, pluggable congestion) · netcode.io (secure connect tokens, per-packet encryption) · laminar
(ENet-like, Rust) · RTP/SRTP + WebRTC (media transport, ULPFEC/FlexFEC, jitter buffer, NACK/PLI) ·
Opus in-band FEC · RFC 5389/8445/5766 (STUN/ICE/TURN).

## 10. Open questions for review

1. **Primary target:** authoritative-server games, P2P games, or media calls first? (Sets whether NAT
   traversal is early or late.)
2. **Media as first-class now, or game-state first** with media as a later profile?
3. **Encryption:** mandatory, optional-on-by-default, or bring-your-own?
4. **Bespoke protocol vs. interop:** a lean custom protocol (max control, matches the ethos), or
   interoperate with QUIC / WebRTC data channels (works with browsers, but heavy)?
5. **Congestion controller:** ship a simple game-grade throttle first, or invest in a real
   media-grade controller up front?

### Recommended default path (pending your call)

If no other direction is given, the coherent default that best fits both "general-purpose" and
elektro-Q's ethos:

1. **Target: truly general, no bias.** Build the neutral L0–L5 core first; defer the media profile
   and NAT traversal until the core is proven. Phase 1 (L0) is useful to every target anyway.
2. **Protocol: bespoke and lean.** A custom protocol keeps the reflection-free, zero-config-native
   story intact — the project's identity. A WebRTC/QUIC *bridge* can be added later if browser reach
   becomes a goal, without disturbing the core.
3. **Encryption: on by default, optional off.** Built-in AEAD once the crypto phase lands (phase 5);
   earlier milestones run plaintext, LAN/benchmark runs can opt out.
4. **Congestion: simple game-grade throttle first,** upgraded to a media-grade controller when the
   media profile (L6) arrives.

Under those defaults the immediate next step is **Phase 1 only**: `UdpTransport` + the network
simulator + metrics — small, self-contained, and independently useful, exactly like the local
transport was.
