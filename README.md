# Atchung!

**Attention! Something happened.**

A realtime, multithreaded **broadcast-and-subscribe event bus** for the JVM and GraalVM
native-image. Publish an event on a topic; every subscriber is notified. Pure Java, no reflection,
no native code, no dependencies — every component (input, graphics, GUI, workers) is just a
producer/consumer on the bus.

## Why it exists

A realtime pub/sub event system is the connective tissue of a desktop application: one user action
fans out to many independent components, each reacting differently. Atchung! is that fabric as a
standalone library, decoupled from any language runtime or transport — so an input middleware, a
graphics engine, and a GUI toolkit can all meet on the same bus without depending on each other.

## Model

| Concept | What it is |
|---------|-----------|
| `Topic<T>` | A typed channel identity (`name` + payload type). Publishers and subscribers rendezvous on equal topics. |
| `Atchung` | The bus. `publish(topic, event)`, `subscribe(...)`, `pump()`. |
| `Subscriber<T>` | `void on(T event)` — the reaction. |
| `Subscription` | `AutoCloseable` handle; close to stop delivery. |
| `Pump` | A per-thread drain point for pumped subscribers. |

## Delivery modes (chosen per subscriber)

```java
Atchung bus = Atchung.create();               // or Atchung.global()
Topic<InputEvent> INPUT = Topic.of("input", InputEvent.class);

// inline — run on the publisher's thread (lowest latency; keep it cheap)
bus.subscribe(INPUT, e -> ...);

// async — run on your executor (background/heavy work)
bus.subscribeAsync(INPUT, e -> ..., workerPool);

// pumped — queue into a bounded mailbox, deliver on YOUR thread when you drain
Pump ui = bus.pump();
ui.subscribe(INPUT, e -> ..., 256, Backpressure.DROP_OLDEST);
// ...once per frame on the render thread:
ui.drain();

bus.publish(INPUT, event);                      // non-blocking, fans out to all three
```

**Publishing never blocks** (except a `BLOCK` mailbox): a fast producer is never stalled by a slow
consumer. When a pumped mailbox is full, `Backpressure` decides: `DROP_OLDEST` (default),
`DROP_NEWEST`, `COALESCE_LATEST` (keep only the newest — ideal for pointer position / window size),
or `BLOCK` (apply upstream backpressure — off the realtime path only).

Ordering is per-topic FIFO from a single publisher. The bus passes event references **without
copying or serialization** — it is pure in-VM.

**Pause / resume.** Any `Subscription` can `pause()` and `resume()` — the primitive for "stop
receiving" without unsubscribing. Pausing is **lossy**: events during a pause are dropped, not
buffered. The bus has no notion of *why* you paused (focus, priority, …) — that lives in your code.

## Two broadcast shapes: events and state

Events answer *"what happened"* — each consumer folds them into its own state. **`State<T>` answers
*"what is true now"***: one producer owns a value, consumers read coherent immutable versions. This
is the shape for things like pointer position or a document model, where only the latest value
matters and lossy pause must stay safe (on resume you just re-read the current version — no
stuck-key staleness).

```java
State.Builder<Camera> b = State.of(new Camera(origin));
Committer<Camera, Vec3>  MOVE = b.mutation("move", (c, d) -> c.movedBy(d));   // declared up front
Committer<Camera, Float> ZOOM = b.mutation("zoom", (c, f) -> c.zoomed(f));
State<Camera> cam = b.history(64, Duration.ofSeconds(2)).build();

// Producer (only the owner) — no ad-hoc writes, only declared commits:
cam.commit(MOVE, delta);                 // atomic -> new immutable version, version++

// Consumers:
Versioned<Camera> now = cam.current();   // poll (lock-free, zero-copy); read as often as you like
Subscription s = cam.onCommit(v -> ...); // react per new version (pausable/lossy)
Versioned<Camera> next = cam.await(now.version()); // block until a newer version
Optional<Versioned<Camera>> old = cam.at(now.version() - 1); // bounded history
```

- **Threadsafe by construction, not obstruction:** lock-free reads (immutable snapshot behind an
  atomic reference), lock-free CAS commit. No mutex on the data path.
- **Atomicity is per-state, single-producer** — no cross-state transactions.
- **Bounded history** (max depth and/or TTL) means no unbounded growth.
- **Forward-designed for remote:** the version number is the delta/keyframe hook, and a named
  commit command is the replicable unit (ship the command *or* the snapshot) — the remote bridge
  adds that machinery without changing this surface.

## Going remote

Atchung! is in-VM by design. To ship events across processes or machines, bridge the bus to a
transport in a **separate** module (e.g. `atchung-elektroq`, planned) — a subscriber that forwards
selected topics onto the wire, and an inbound adapter that re-publishes them locally. The core stays
pure and fast; network transparency is opt-in and never taxes local delivery.

## Requirements

- JDK 25+ (enforced by `maven-enforcer-plugin`).
- No runtime dependencies; native-image clean (no reflection, no `ServiceLoader`, no native code).
