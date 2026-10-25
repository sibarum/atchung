# elektro-Q shared-memory transport: design proposal

**Status:** proposed, 2026-10-24. Nothing built. A same-machine transport that moves large frames through a mapped
region both peers can see, and keeps a Unix-domain socket for ordering, wake-ups and peer lifecycle. It works on Linux
and Windows. The first user is the vexelray-os compositor (`vexelray-os/docs/m0-spike.md`), whose spike did this by hand:
48-byte control messages on a socket, instance batches through a file in `/dev/shm`. Made a transport, it is reusable
by anything that already speaks elektro-Q on one machine, such as the Inspector link.

---

## 1. What it is for

`UdsTransport` copies every frame twice through the kernel (sender to socket buffer, socket buffer to receiver), once
more through `BufferedInputStream`, and allocates a `byte[]` per frame on each side (`drain` in `send`, `readLoop` on
receive). For control messages that is nothing. For a 134 KB first batch from a text-heavy window, a stream of
4 KB instance batches at 60 Hz, or later images, it is copying and garbage that buys nothing on one machine.

The goal is not zero-copy in Atchung's sense. Atchung's in-VM bus passes the object itself; there are no bytes. Across
processes a message must be written once into memory both sides can see. What this transport removes is everything
after that write: the kernel copies, the socket buffer pressure, and (in later phases) the decode copy.

Non-goals: crossing machines (that is TCP and netcode), and replacing `UdsTransport` for small-message traffic, where
it is already as cheap as this would be.

## 2. Principles

1. **The SPI does not change for phase 1.** `Transport` already names shared memory as a medium it is meant for, and
   `FrameListener` already says a frame is "valid only for the duration of the call". That contract is exactly what
   lets a receiver read a frame in place and release it on return.
2. **The socket is the ordering authority.** Every frame, inline or in the region, is announced on the socket in
   order. Per-peer ordering, which actors rely on, then holds by construction, and inline and region frames mix
   freely.
3. **No spinning.** The socket is the doorbell. A frame costs one small socket write and read, as now; only the bulk
   bytes stop going through the kernel.
4. **The peer is untrusted.** The compositor's programs are sandboxed. Nothing the peer writes into the region (counters,
   lengths, bytes) is believed without a bounds check, and the peer must not be able to crash the reader (§6).
5. **The listener owns the region.** The side that listens creates the region and hands it to the side that dials.
   In the compositor that is the more privileged side, which matters on both platforms (§5).
6. **Portable first, native second.** A file-backed region needs no FFM and works on both platforms for trusted peers.
   The sealed, handle-passed regions that untrusted peers need come after, behind the same interface.

## 3. Wire

### 3.1 The region

One region per connection, created by the listener at accept, holding two rings: listener-to-dialer and
dialer-to-listener. Each ring:

```
page 0 (consumer's)   release position u64   (written by the consumer only)
page 1 (producer's)   magic u32, layout version u16, flags u16, capacity u64 (data area size, a power of two),
                      then write position u64 on its own cache line (written by the producer only)
page 2...             data area[capacity]
```

The release position sits on a page of its own so that the consumer can map everything else read-only (§6).

Positions are monotonic 64-bit byte counters; the offset in the data area is `position & (capacity - 1)`. A frame is
stored contiguously, at an 8-byte-aligned position, preceded by a `u32` length. When it would not fit before the end of
the data area, the producer writes a wrap marker and starts at offset 0. The free space is
`capacity - (write - release)`.

Positions are published with release stores and read with acquire loads (`VarHandle` over the mapped
`MemorySegment`), which order correctly between processes on a shared mapping.

### 3.2 The socket stream

The socket carries length-delimited records, each one of:

| Kind | Body | Meaning |
|---|---|---|
| `INLINE` | the frame bytes | a small frame, sent through the socket as `UdsTransport` does |
| `REF` | ring position u64, length u32 | a frame stored in the ring at that position |
| `REGION` | how to map the region (§5) | sent once by the listener after accept |
| `RELEASE` | nothing | "I advanced my release position", sent only when the producer is waiting for space |

The receiver handles records strictly in order. For a `REF` it checks the position and length (§6), wraps the bytes in
a read-only `ByteBuffer` slice of the mapping, calls the `FrameListener`, and on return advances its release position.
Frames are released in the order they arrived, so the release position only moves forward.

### 3.3 Choosing inline or region

Per frame: at or below an inline threshold (default 1 KiB), `INLINE`. Above it, `REF` if the ring has room. When the
ring is full, the sender sets a waiting flag, which makes the consumer send a `RELEASE` record at its next release, and
waits up to the existing `sendTimeout`. Past that, the peer is dropped, as `UdsTransport` drops a peer whose writes stay
blocked. A frame larger than the ring always goes `INLINE`, bounded by the same 64 MiB maximum frame size.

The default ring is 4 MiB per direction, configurable per transport.

## 4. Phases

### Phase 1: the transport alone

`elektroq-transport-shm`: `ShmTransport` and an `ElektroShm` factory, mirroring `ElektroUds`. The SPI and
`DefaultConduit` are unchanged.

- Send: the conduit still encodes into a heap `IbWriter` and passes a heap buffer; the transport copies it into the
  ring. One copy, against today's three plus an allocation.
- Receive: the listener gets a direct buffer. `DefaultConduit.onFrame` already copies a buffer with no backing array
  into a `byte[]` before decoding, so this works unchanged at the cost of one copy.

So phase 1 is two user-space copies per frame and no kernel copy of the payload. Worth measuring against `UdsTransport`
before going further (§8): if the savings are small at real sizes, phase 2 can wait.

### Phase 2: decode in place

Remove the receive-side copy. This needs readers that work over off-heap memory: `IbReader` in intrabuf and
`WireBufferReader` in elektroq-core are `byte[]` only today. With a `MemorySegment`-backed reader, `onFrame` decodes
straight from the ring.

The send-side copy stays. Encoding in place would need the conduit to claim a slot before it knows the encoded size,
and a few kilobytes of `memcpy` cost microseconds. Not worth an SPI change unless measured.

### Phase 3: bulk fields

The real win for the compositor and for images. Decoding still turns a frame into Java objects; an instance batch
decoded into a `float[]` is another copy, and the compositor only wants to upload it to the GPU. A **bulk field** is a
message field whose value is a view of bytes rather than decoded data:

```java
@Message(id = 40)
record InstanceBatch(long frame, int removals, Bulk instances) { }
```

- **Sending**, the size of a bulk field is known up front, so this is where claiming does fit: the sender asks for a
  slot of that size, fills it (for the compositor's programs, writes instances straight into the ring), and emits.
- **Receiving**, the actor gets a read-only view into the ring, valid for the duration of `react` (the same lifetime
  rule as the frame). The compositor uploads from it to its instance buffer directly.
- **Over any other transport**, `Bulk` is an ordinary length-prefixed byte field: TCP and UDS peers still work, they
  just copy.

Further out, if the ring's memory is also imported into Vulkan as host memory (`VK_EXT_external_memory_host`), the GPU
can read batches where the program wrote them, without an upload. That is the compositor's business, not this
transport's; the transport only has to allow a region to be allocated with the alignment the import needs.

## 5. Platforms

The socket is `AF_UNIX` on both: Java's `SocketChannel` supports it on Windows 10 1803 and later, and the Inspector
link already runs on it there. What differs is how the region is created and handed over. A `Region` interface with
three backends:

| Backend | Platforms | Created and handed over | Peers |
|---|---|---|---|
| `FileRegion` | Linux, Windows | a file in a per-user private directory (`$XDG_RUNTIME_DIR` or `/dev/shm`; `%LOCALAPPDATA%\Temp`), mapped with `FileChannel.map`; the path goes in the `REGION` record | trusted only (§6) |
| `MemfdRegion` | Linux | `memfd_create`, sized, then sealed against shrinking and growing; the fd goes over the socket with `SCM_RIGHTS` | untrusted |
| `SectionRegion` | Windows | an unnamed pagefile-backed `CreateFileMappingW`; `DuplicateHandle` into the dialer's process, and the handle value goes in the `REGION` record | untrusted |

`FileRegion` needs no FFM and is what phase 1 ships with. The other two are vexelray-os's M1 ("file descriptors over a
socket"), built here for the transport's own region. Handing a peer arbitrary resources (dma-bufs, sync files) is a
wider seam that this does not try to be, but it will reuse both mechanisms.

Two consequences:

- **Linux: the transport must own the socket's fd.** Java's `SocketChannel` does not expose it, and `sendmsg` with
  `SCM_RIGHTS` needs it. `MemfdRegion` therefore means a socket opened through FFM (`socket`, `bind`, `listen`,
  `accept`, `sendmsg`, `recvmsg`), not through `SocketChannel`. Windows `AF_UNIX` has no `SCM_RIGHTS`, so Windows keeps
  `SocketChannel` and duplicates handles instead.
- **Windows: the listener needs a handle to the dialer's process** to duplicate into it. It gets the peer's process id
  from the socket (`SIO_AF_UNIX_GETPEERPID`), not from anything the peer says. This is why the listener owns the
  region: a sandboxed program in an AppContainer cannot open the compositor's process, but the compositor can open
  the program's.

## 6. Hostile peers

What a peer can do to shared memory, and what stops it:

| Attack | Defence |
|---|---|
| Write a bad length or position into a `REF` or the ring | Every `REF` is checked: position between the consumer's release position and the producer's published write position, length at most the capacity and equal to the stored `u32`, frame inside the data area. A failure is a framing fault: drop the peer, as `UdsTransport` does. |
| Change bytes while the receiver decodes them | Java reads are bounds-checked, so the worst case is garbage values, which intrabuf's checked decode turns into an exception and a dropped frame. A decoder must never use a length it read twice from shared memory as if the two reads agreed. |
| Write into the other direction's ring or header | A consumer maps its peer's producer pages (header and data) read-only, and only its own release page writable. A producer cannot be stopped from writing the consumer's release page, so the consumer keeps its release position authoritative in local memory and only publishes it there; a producer that lies about free space only corrupts its own frames, which the `REF` checks catch. |
| Shrink the backing file, so touching a page past the new end raises `SIGBUS` and kills the JVM | `MemfdRegion` seals the memfd against shrinking before handing it over. A Windows section cannot be shrunk while mapped. `FileRegion` has no defence, which is why it is for trusted peers only. |
| Never release, to stall the sender | `sendTimeout`, as today. |

## 7. Compositor mapping

What the M0 spike becomes on this transport:

| Spike today | On `ShmTransport` |
|---|---|
| Hand-rolled 48-byte control messages | `@Message` records, versioned and checked (closes the spike's first shim) |
| A `/dev/shm` file per program, opened by path | the transport's region: `FileRegion` first, `MemfdRegion` or `SectionRegion` for sandboxed programs |
| "A program cannot write until its wake is answered", so it never overwrites a batch in use | the ring's release position: a batch's slot is released when the compositor's actor returns from applying it |
| Batches copied from the file into the instance buffer | phase 1 to 2: decoded from the ring; phase 3: `Bulk`, uploaded straight from the ring |

D5 (one batch per program per frame) and D14 (hold an early batch until its frame) are compositor rules and stay in the
compositor. D14's "hold" changes meaning slightly: a held batch must be copied out of the ring or its slot stays
unreleased, which would stall the program. Copying is right, since holding is rare (one to two batches per phase in the
spike).

## 8. Testing and measurement

- **Ring unit tests:** wrap, full ring, waiting and `RELEASE`, release order, a frame larger than the ring, and every
  row of §6 as a hostile-peer test that writes bad values into a real mapping.
- **Cross-process tests** on both platforms, in the same shape as `UdsTransport`'s.
- **A benchmark against `UdsTransport`** at 48 B, 1 KB, 4 KB, 134 KB and 4 MB: latency at p50 and p99, throughput, and
  allocation per frame. This decides whether phase 2 is worth doing.
- **The M0 timeline**, rerun on the new transport, with wake-to-present and apply-on-arrival compared to the spike's
  tables.

## 9. Open questions

1. **Native image.** elektro-Q builds a native image with zero reachability configuration. FFM downcalls (for
   `MemfdRegion` and `SectionRegion`) need registering for native-image. Phase 1 (`FileRegion`) needs none, so the
   question can wait, but the zero-configuration promise may have to become "zero for the portable backends".
2. **Where `Bulk` lives.** It is a field type, so it touches codegen and the wire readers. Is it an elektro-Q type, or
   an intrabuf one with elektro-Q's transports giving it its zero-copy meaning?
3. **One module or two.** Phase 1 could be a mode of `UdsTransport` (it already owns the socket and its framing), or
   a separate `elektroq-transport-shm` that depends on it. A separate module keeps `UdsTransport` small and FFM-free;
   the cost is some duplicated connection handling.
4. **macOS.** `AF_UNIX` with `SCM_RIGHTS` exists, but there is no `memfd_create`; `shm_open` plus unlinking is the
   nearest. Not needed now; the `Region` interface leaves room for it.
