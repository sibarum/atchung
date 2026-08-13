package sibarum.elektro.queue.transport;

import sibarum.elektro.queue.message.PeerId;

/**
 * Notified as peers join and leave a {@link Transport}.
 *
 * <p>A conduit uses these callbacks to maintain its connection pool and to drive
 * request/response cleanup when a peer disappears mid-exchange.
 */
public interface PeerListener {

    /** A new peer connection has been established and assigned {@code peer}. */
    void onConnected(PeerId peer);

    /** The connection for {@code peer} has been lost or closed. */
    void onDisconnected(PeerId peer);
}
