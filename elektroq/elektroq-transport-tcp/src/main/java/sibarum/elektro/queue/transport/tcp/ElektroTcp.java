package sibarum.elektro.queue.transport.tcp;

import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.MessageRegistry;

/**
 * Convenience factory wiring a {@link DefaultConduit} onto a {@link TcpTransport}.
 *
 * <p>For the common cases &mdash; a server on a known port, or a client dialing one
 * &mdash; these one-liners save constructing the transport and conduit separately.
 * When you need the ephemeral bound port or finer control, build the {@link TcpTransport}
 * directly and pass it to {@code new DefaultConduit(...)}.
 */
public final class ElektroTcp {

    private ElektroTcp() {
    }

    /** A conduit that listens for peers on {@code port}. */
    public static Conduit server(String name, int port, MessageRegistry registry) {
        return new DefaultConduit(name, TcpTransport.listening(port), registry);
    }

    /** A conduit that connects to a server at {@code host:port}. */
    public static Conduit client(String name, String host, int port, MessageRegistry registry) {
        return new DefaultConduit(name, TcpTransport.connecting(host, port), registry);
    }
}
