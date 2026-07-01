package sibarum.elektro.queue.transport;

import sibarum.elektro.queue.message.PeerId;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * The service-provider seam that keeps elektro-Q transport-agnostic.
 *
 * <p>A {@code Transport} moves opaque, already-framed byte buffers (header + payload)
 * between peers and reports peer lifecycle; it knows nothing about message types,
 * codecs, or the registry. A {@link sibarum.elektro.queue.Conduit} sits on top of one
 * transport and supplies the meaning. This is the only interface a new medium &mdash;
 * TCP, UDP, shared memory, a STUN/TURN relay &mdash; must implement, which is what lets
 * elektro-Q grow from IPC into a netcode framework without touching the core triad.
 *
 * <p>Frames handed to {@link #send} are complete and ordering is the transport's
 * concern; a datagram transport may additionally set fragment flags. Implementations
 * must be safe to call {@link #send} from multiple threads.
 */
public interface Transport extends AutoCloseable {

    /** Brings the transport up; completes when it is ready to send and receive. */
    CompletionStage<Void> start();

    /**
     * Sends one framed buffer to {@code destination} (or all peers when
     * {@link PeerId#BROADCAST}). The buffer's readable region is consumed; callers
     * must not mutate it afterwards.
     */
    void send(PeerId destination, ByteBuffer frame);

    /** Installs the callback invoked for each inbound frame. */
    void onFrame(FrameListener listener);

    /** Installs the callback invoked as peers connect and disconnect. */
    void onPeer(PeerListener listener);

    /** Shuts the transport down and releases all connections. Idempotent. */
    @Override
    void close();
}
