package sibarum.elektro.queue.transport.local;

import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.MessageRegistry;

/**
 * Convenience factory wiring a {@link DefaultConduit} onto a {@link LocalTransport}.
 *
 * <p>The in-VM counterpart to {@code ElektroTcp}: a server publishes a named endpoint and a client
 * dials it, all within one JVM. The conduit API above is identical to the TCP case, so code written
 * against one transport moves to the other unchanged &mdash; the whole point of the SPI seam.
 */
public final class ElektroLocal {

    private ElektroLocal() {
    }

    /** A conduit that publishes {@code endpoint} for local clients to dial. */
    public static Conduit server(String name, String endpoint, MessageRegistry registry) {
        return new DefaultConduit(name, LocalTransport.listening(endpoint), registry);
    }

    /** A conduit that connects to a local server published at {@code endpoint}. */
    public static Conduit client(String name, String endpoint, MessageRegistry registry) {
        return new DefaultConduit(name, LocalTransport.connecting(endpoint), registry);
    }
}
