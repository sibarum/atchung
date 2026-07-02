package sibarum.elektro.queue.transport.udp;

import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.MessageRegistry;

/**
 * Convenience factory wiring a {@link DefaultConduit} onto a {@link UdpTransport}.
 *
 * <p>The UDP counterpart to {@code ElektroTcp}. Note the guarantees differ: this is raw, unreliable,
 * unordered datagram delivery (docs/netcode-design.md, layer 0) &mdash; suitable for fire-and-forget
 * state you resend anyway, not for request/reply, which needs the reliability layer above.
 */
public final class ElektroUdp {

    private ElektroUdp() {
    }

    /** A conduit that receives datagrams on {@code port}, discovering peers as they arrive. */
    public static Conduit server(String name, int port, MessageRegistry registry) {
        return new DefaultConduit(name, UdpTransport.listening(port), registry);
    }

    /** A conduit that sends datagrams to a server at {@code host:port}. */
    public static Conduit client(String name, String host, int port, MessageRegistry registry) {
        return new DefaultConduit(name, UdpTransport.connecting(host, port), registry);
    }
}
