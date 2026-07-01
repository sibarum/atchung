package sibarum.elektro.queue;

import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;

import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * The managing corner of elektro-Q's triad: owns a transport and its pool of peer
 * connections, and hands out {@link Action}s and {@link Subscription}s over it.
 *
 * <p>A conduit is the boundary between the transport-agnostic core and a concrete
 * transport (TCP now, UDP or shared memory later): callers build {@link Action}s to
 * emit and register {@link Actor}s to react, while the conduit handles framing,
 * connection pooling, peer lifecycle, and dispatch. The same {@link MessageRegistry}
 * that lets it decode inbound frames is shared with the codecs used to emit.
 *
 * <p>Obtain a conduit from a transport-specific builder, then {@link #start()} it.
 * Closing releases every connection and deactivates outstanding subscriptions.
 */
public interface Conduit extends AutoCloseable {

    /** Human-readable name for diagnostics and logging. */
    String name();

    /** Current lifecycle state. */
    ConduitState state();

    /** The registry of message types this conduit can emit and decode. */
    MessageRegistry registry();

    /** Brings the transport up. The stage completes once the conduit is {@code OPEN}. */
    CompletionStage<Void> start();

    /** Returns an emitter for {@code type}. Actions are cheap to obtain and reusable. */
    <T> Action<T> action(MessageType<T> type);

    /** Registers {@code actor} to react to messages of {@code type}. */
    <T> Subscription subscribe(MessageType<T> type, Actor<T> actor);

    /** Snapshot of the currently connected peers. */
    Set<PeerId> peers();

    /** Closes the transport and all connections. Idempotent. */
    @Override
    void close();
}
