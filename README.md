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

## Going remote

Atchung! is in-VM by design. To ship events across processes or machines, bridge the bus to a
transport in a **separate** module (e.g. `atchung-elektroq`, planned) — a subscriber that forwards
selected topics onto the wire, and an inbound adapter that re-publishes them locally. The core stays
pure and fast; network transparency is opt-in and never taxes local delivery.

## Requirements

- JDK 25+ (enforced by `maven-enforcer-plugin`).
- No runtime dependencies; native-image clean (no reflection, no `ServiceLoader`, no native code).
