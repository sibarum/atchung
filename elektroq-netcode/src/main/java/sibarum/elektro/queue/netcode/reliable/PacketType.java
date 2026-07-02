package sibarum.elektro.queue.netcode.reliable;

/**
 * The kind of a netcode packet, carried in the packet header's {@code type} byte. Distinct from the
 * message-envelope {@code Flags} — this is the layer <em>below</em> the envelope (docs/netcode-design.md).
 *
 * <ul>
 *   <li>{@link #DATA} — carries an application payload (a framed message).</li>
 *   <li>{@link #CONNECT}/{@link #ACCEPT} — the two-step handshake that establishes a session.</li>
 *   <li>{@link #KEEPALIVE} — no payload; keeps the session alive and carries fresh ack info when
 *       there is no data to piggyback on.</li>
 *   <li>{@link #DISCONNECT} — a graceful teardown notice.</li>
 * </ul>
 */
public final class PacketType {

    public static final int DATA = 0;
    public static final int CONNECT = 1;
    public static final int ACCEPT = 2;
    public static final int KEEPALIVE = 3;
    public static final int DISCONNECT = 4;

    private PacketType() {}
}
