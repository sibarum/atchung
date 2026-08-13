# elektro-Q

A fast, reflection-free message-passing library for the JVM and **GraalVM native-image**.

elektro-Q moves typed messages between two processes — on the same machine or across a
network — with no reflection, no runtime scanning, and a JDK-only core. Message codecs
are generated at compile time from annotated records, so the whole stack compiles to a
native binary with **zero reachability configuration**. It's designed to slot in as an
add-on for DSLs written with [Truffle](https://www.graalvm.org/latest/graalvm-as-a-platform/language-implementation-framework/),
and its transport is pluggable — TCP, an in-VM local transport, and a UDP netcode stack
today, with room to grow toward STUN/TURN P2P.

The model is a triad:

> **Actions emit. Conduits manage connection pools. Actors react to events.**

---

## Highlights

- **No reflection.** Type identity, dispatch, and (de)serialization run on generated code
  and integer ids. Native-image needs no `reflect-config.json`.
- **Compile-time codecs.** Annotate a `record`; the processor emits a hand-rolled binary
  `Codec` and a registrar. No hand-written serialization, no schema files.
- **Transport-agnostic.** Everything transport-specific sits behind a small SPI. Bundled
  transports: TCP (blocking sockets on virtual threads), Unix domain sockets (the fast
  same-machine path), an in-VM local transport, and a UDP transport with a layered
  real-time netcode stack on top.
- **Versioned by construction.** Stable message ids plus per-field schema versioning let
  peers on different builds interoperate.
- **Request/reply built in**, with correlation, request timeouts, and disconnect-aware
  failure so a call never hangs forever.
- **Native-proven.** A self-verifying binary exercises the full stack; `mvn -Pnative verify`
  builds and runs it.

## Requirements

- **JDK 25** (records, virtual threads). A **GraalVM** JDK is required only for the native
  build; the library and its tests run on any JDK 25.
- **Maven 3.9+**.
- For native builds on Windows, the **MSVC toolchain** (Visual Studio Build Tools). GraalVM
  auto-detects it.

## Modules

| Module | What it provides |
|---|---|
| `elektroq-core` | The triad (`Action`/`Conduit`/`Actor`), the wire/codec contract, the message envelope, the transport SPI, and runtime pieces (`DefaultConduit`, `ArrayMessageRegistry`, buffer codecs). JDK-only. |
| `elektroq-codegen` | The annotation processor that turns `@Message` records into `Codec`s + a registrar. Compile-time only. |
| `elektroq-transport-tcp` | `TcpTransport` — a TCP implementation of the transport SPI — and the `ElektroTcp` convenience factory. |
| `elektroq-transport-uds` | `UdsTransport` — a Unix-domain-socket implementation of the SPI (fast same-machine IPC, skips the TCP/IP stack) — and the `ElektroUds` convenience factory. |
| `elektroq-transport-local` | An in-VM transport that wires two conduits together without sockets — handy for tests and single-process setups. |
| `elektroq-netcode` | UDP transport (`UdpTransport`/`ElektroUdp`) plus a layered real-time netcode stack: connection, sequencing + acks, independent channels with per-message delivery modes, and a built-in network simulator. |
| `elektroq-example` | End-to-end demo and the native-image smoke test. |

---

## Architecture in one screen

**Wire format.** Every frame is a fixed **24-byte header** followed by a payload:

```
magic(2=0xEC51) | version(1) | flags(1) | typeId(4) | schemaVersion(4) | correlationId(8) | payloadLength(4)
```

The header tells a receiver how to route a frame (`typeId` → registry lookup) before
decoding the payload. Codecs write primitives big-endian and use LEB128 var-ints for
compact lengths.

**Framing vs. transport.** A `Conduit` builds the header+payload frame and hands the
opaque bytes to a `Transport`. The transport is oblivious to the header — the TCP
transport does its own length-delimiting (a 4-byte prefix) to split the stream. That
separation is what keeps the core transport-agnostic.

**Dispatch.** Inbound, the conduit decodes the header, looks the type up by integer id in
the `MessageRegistry`, decodes the payload, and either completes a pending `request`
future (on a reply) or fans out to the subscribed `Actor`s. Actors run on the transport's
receive thread (a per-connection virtual thread for TCP), which preserves per-peer
ordering — so **actors must not block indefinitely**.

---

## Tutorial: integrate elektro-Q

We'll build a tiny ping/greeting service: a client emits a one-way `Greeting` and issues a
`Ping` request that the server answers with a `Pong`.

### 1. Build and install the library locally

elektro-Q isn't published to Maven Central yet, so install it into your local repository:

```bash
git clone <your-fork-or-copy> elektroq
cd elektroq
mvn install            # builds all modules, runs the tests, installs 1.0-SNAPSHOT
```

### 2. Depend on the runtime modules

In your application's `pom.xml`:

```xml
<dependencies>
  <dependency>
    <groupId>sibarum.elektro.queue</groupId>
    <artifactId>elektroq-core</artifactId>
    <version>1.0-SNAPSHOT</version>
  </dependency>
  <dependency>
    <groupId>sibarum.elektro.queue</groupId>
    <artifactId>elektroq-transport-tcp</artifactId>
    <version>1.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

### 3. Wire in the codec generator

> **Important:** Since JDK 23, `javac` no longer auto-discovers annotation processors on
> the classpath. You **must** declare the generator on the processor path, or no codecs
> are generated. Declaring it here also keeps it out of your runtime/native image.

```xml
<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-compiler-plugin</artifactId>
      <configuration>
        <annotationProcessorPaths>
          <path>
            <groupId>sibarum.elektro.queue</groupId>
            <artifactId>elektroq-codegen</artifactId>
            <version>1.0-SNAPSHOT</version>
          </path>
          <path>
            <groupId>sibarum.elektro.queue</groupId>
            <artifactId>elektroq-core</artifactId>
            <version>1.0-SNAPSHOT</version>
          </path>
        </annotationProcessorPaths>
      </configuration>
    </plugin>
  </plugins>
</build>
```

### 4. Define your messages

Messages are **top-level `record`s** annotated with `@Message`, each carrying a **stable,
unique `id`**. Supported field types are the primitives, `String`, `byte[]`, and other
`@Message` records (nested messages).

```java
package com.example.chat;

import sibarum.elektro.queue.message.Message;

@Message(id = 1)
public record Greeting(String text) { }

@Message(id = 2)
public record Ping(long seq) { }

@Message(id = 3)
public record Pong(long seq, String note) { }
```

For each one, the processor generates (in the same package) a `Codec` with two static
handles you'll use:

- `GreetingCodec.INSTANCE` — the codec itself (rarely used directly).
- `GreetingCodec.TYPE` — a `MessageType<Greeting>` bundling the id, schema version, and
  codec. **This is the token you pass to `action(...)` and `subscribe(...)`.**

It also generates one aggregating registrar:
`sibarum.elektro.queue.generated.ElektroRegistrar`.

### 5. Build a registry

The registry maps incoming type ids to their codecs. Register every message type once at
startup — the generated registrar does it in a single call:

```java
import sibarum.elektro.queue.generated.ElektroRegistrar;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;

MessageRegistry registry = new ArrayMessageRegistry();
ElektroRegistrar.registerAll(registry);   // registers Greeting, Ping, Pong
```

Both ends of a connection need the same types registered.

### 6. Stand up the server

```java
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.transport.tcp.TcpTransport;
import com.example.chat.*;

var transport = TcpTransport.listening(7000);
var server = new DefaultConduit("server", transport, registry);
server.start().toCompletableFuture().join();   // OPEN once the socket is bound

// React to one-way greetings:
server.subscribe(GreetingCodec.TYPE, (greeting, ctx) ->
        System.out.println("greeting from peer " + ctx.source().handle() + ": " + greeting.text()));

// Answer Ping requests with a correlated Pong:
server.subscribe(PingCodec.TYPE, (ping, ctx) ->
        ctx.reply(PongCodec.TYPE, new Pong(ping.seq(), "ack")));
```

`ctx.reply(...)` sends a reply correlated to the request, back to the peer that sent it —
you don't build an `Action` for it.

### 7. Connect a client and talk

```java
import sibarum.elektro.queue.message.PeerId;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

// The 4-arg constructor sets a request timeout so calls can't hang forever.
var client = new DefaultConduit("client",
        TcpTransport.connecting("127.0.0.1", 7000), registry, Duration.ofSeconds(5));
client.start().toCompletableFuture().join();

// Fire-and-forget. With no destination, emit() broadcasts to every connected peer
// (for a client with one upstream, that's the server).
client.action(GreetingCodec.TYPE).emit(new Greeting("hello elektro-Q"));

// Request/reply. Pick the peer to talk to; a single-upstream client has exactly one.
PeerId server = client.peers().iterator().next();
CompletionStage<Pong> pong = client.action(PingCodec.TYPE)
        .request(new Ping(42L), server, PongCodec.TYPE);

pong.thenAccept(p -> System.out.println("pong: seq=" + p.seq() + " note=" + p.note()));
```

### 8. Shut down

```java
client.close();   // idempotent; fails any in-flight requests
server.close();
```

### Shortcut: `ElektroTcp`

For fixed-port server/client wiring, skip the transport boilerplate:

```java
import sibarum.elektro.queue.transport.tcp.ElektroTcp;

var server = ElektroTcp.server("server", 7000, serverRegistry);
var client = ElektroTcp.client("client", "127.0.0.1", 7000, clientRegistry);
```

(When you need the ephemeral bound port — e.g. `listening(0)` in tests — build the
`TcpTransport` directly and read `transport.boundPort()` after `start()`.)

A complete, runnable version of all of this lives in
[`elektroq-example/src/main/java/.../NativeDemo.java`](elektroq-example/src/main/java/sibarum/elektro/queue/example/NativeDemo.java).

---

## Message versioning

Message **ids are the contract** between peers and must never be reused or renumbered.
Evolve the *shape* of a message with schema versions instead.

Annotate fields with `@WireField` to control layout and compatibility. It's all-or-none:
annotate every component of a record, or none (in which case declaration order is used).

```java
@Message(id = 2, schemaVersion = 2)
public record Ping(
        @WireField(order = 1) long seq,
        @WireField(order = 2) String origin,
        @WireField(order = 3, since = 2, optional = true) long deadlineMillis) { }
```

- `order` — position on the wire, independent of declaration order.
- `since` — the schema version a field first appeared in.
- `optional` — the field may be absent in payloads from older peers.

A field that is `optional` or `since > 1` is read behind a "bytes remaining?" guard and
**must be trailing** (the processor enforces this). The effect: a newer decoder reading an
older, shorter payload fills such fields with defaults, and an older decoder harmlessly
ignores trailing bytes it doesn't understand.

---

## Native image

The example module ships a `native` Maven profile:

```bash
mvn -Pnative verify
```

This builds a native binary of `NativeDemo` (`--no-fallback`) and then runs it during the
`verify` phase; the binary exits non-zero on any failed check, so a regression fails the
build. On Windows the profile auto-appends the `.exe` suffix.

To build a native image of **your own** app, point the GraalVM
[`native-maven-plugin`](https://graalvm.github.io/native-build-tools/) at your main class.
Because elektro-Q uses no reflection, you shouldn't need any reachability metadata for the
messaging layer itself.

**CI tip:** keep native behind the profile so ordinary `mvn verify` stays fast and
toolchain-free; run `mvn -Pnative verify` in a separate job with
[`graalvm/setup-graalvm`](https://github.com/graalvm/setup-graalvm) (it provisions GraalVM
and, on Windows, the MSVC shell).

---

## Build & test

```bash
mvn verify              # compile + all unit/integration tests (any JDK 25)
mvn install             # + install 1.0-SNAPSHOT into your local repo
mvn -Pnative verify     # + build and run the native smoke test (GraalVM required)
```

---

## Status & roadmap

Working today: the core triad, compile-time codecs (primitives, `String`, `byte[]`, nested
messages), the TCP and in-VM local transports, request/reply with timeouts and disconnect
handling, and a native-image build.

The `elektroq-netcode` module adds a UDP transport and a layered real-time stack, built
out in phases (see [`docs/netcode-design.md`](docs/netcode-design.md)):

- **L0 — `UdpTransport`:** raw datagrams behind the transport SPI (fire-and-forget works
  immediately), plus `ElektroUdp` convenience wiring and a network simulator
  (latency/jitter/loss/reorder/dup) for deterministic tests.
- **L2 — reliability:** packet sequencing, ack bitfields, RTT/loss tracking, and a
  connection/keepalive layer.
- **L3 — channels + delivery modes:** independent channels with no cross-stream
  head-of-line blocking, and a per-message `DeliveryMode` — `UNRELIABLE`,
  `UNRELIABLE_SEQUENCED`, `RELIABLE_UNORDERED`, or `RELIABLE_ORDERED`.

Natural next steps: fragmentation/reassembly (L4), congestion control (L5), a media
profile (L6), a Truffle add-on, and backpressure controls.

## License

Part of the **Pontif** project. Dual-licensed:

- **Source code** — Apache License 2.0 (see [`LICENSE`](LICENSE)).
- **Documentation** (including this README) — CC BY 4.0 (see [`LICENSE-docs`](LICENSE-docs)).
