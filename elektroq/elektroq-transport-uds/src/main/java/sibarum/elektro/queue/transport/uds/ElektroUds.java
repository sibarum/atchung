package sibarum.elektro.queue.transport.uds;

import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.MessageRegistry;

import java.nio.file.Path;

/**
 * Convenience factory wiring a {@link DefaultConduit} onto a {@link UdsTransport}.
 *
 * <p>The same-machine counterpart to {@code ElektroTcp}: a server binds a socket file and a client
 * dials it. The conduit API is identical to the TCP case, so code written against one transport
 * moves to the other unchanged &mdash; the point of the SPI seam.
 */
public final class ElektroUds {

    private ElektroUds() {
    }

    /** A conduit that binds {@code socketPath} for local clients to dial. */
    public static Conduit server(String name, Path socketPath, MessageRegistry registry) {
        return new DefaultConduit(name, UdsTransport.listening(socketPath), registry);
    }

    /** A conduit that connects to a server bound at {@code socketPath}. */
    public static Conduit client(String name, Path socketPath, MessageRegistry registry) {
        return new DefaultConduit(name, UdsTransport.connecting(socketPath), registry);
    }
}
