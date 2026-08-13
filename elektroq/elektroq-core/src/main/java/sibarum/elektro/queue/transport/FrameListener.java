package sibarum.elektro.queue.transport;

import sibarum.elektro.queue.message.PeerId;

import java.nio.ByteBuffer;

/**
 * Receives inbound framed buffers from a {@link Transport}.
 *
 * <p>The conduit installs one of these to decode the header, look the type up in its
 * registry, and dispatch to the matching {@link sibarum.elektro.queue.Actor}. The
 * {@code frame} buffer is owned by the transport and valid only for the duration of the
 * call; the listener must copy anything it needs to retain.
 */
@FunctionalInterface
public interface FrameListener {

    void onFrame(PeerId source, ByteBuffer frame);
}
