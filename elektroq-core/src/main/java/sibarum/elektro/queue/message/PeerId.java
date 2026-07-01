package sibarum.elektro.queue.message;

/**
 * Opaque identity of a peer endpoint within a {@link sibarum.elektro.queue.Conduit}.
 *
 * <p>A {@code PeerId} is a compact handle assigned by the conduit when a connection
 * is established; it is cheap to compare, hash, and route on, and carries no network
 * address so the core stays transport-agnostic. Mapping a handle back to a concrete
 * address (a socket, a shared-memory ring, a relay session) is the transport's job.
 *
 * <p>{@link #BROADCAST} is a reserved handle meaning &ldquo;every connected peer&rdquo;.
 */
public record PeerId(long handle) {

    /** Reserved handle addressing all peers of a conduit. */
    public static final PeerId BROADCAST = new PeerId(0L);

    public boolean isBroadcast() {
        return handle == BROADCAST.handle;
    }
}
