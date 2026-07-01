package sibarum.elektro.queue.message;

/**
 * Bit flags carried in the envelope header, describing how a frame should be treated.
 *
 * <p>Flags are protocol-level hints independent of any particular message type: they
 * drive request/response correlation, fragment reassembly, and payload compression.
 */
public final class Flags {

    /** No flags set. */
    public static final byte NONE = 0;

    /** Sender expects a correlated reply; see {@code correlationId}. */
    public static final byte REQUEST = 1;

    /** This frame is a reply correlated to an earlier {@link #REQUEST}. */
    public static final byte REPLY = 1 << 1;

    /** Payload is one fragment of a larger message awaiting reassembly. */
    public static final byte FRAGMENT = 1 << 2;

    /** Payload bytes are compressed. */
    public static final byte COMPRESSED = 1 << 3;

    private Flags() {
    }

    public static boolean has(byte flags, byte flag) {
        return (flags & flag) != 0;
    }
}
