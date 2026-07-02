package sibarum.elektro.queue.netcode.channel;

/**
 * The per-message delivery guarantee — the central abstraction of the netcode layer
 * (docs/netcode-design.md). Chosen per message; independent channels never block one another, so a
 * stalled reliable-ordered stream can't freeze unrelated position updates.
 */
public enum DeliveryMode {

    /** Best-effort; may be lost, may arrive out of order. Cheapest. */
    UNRELIABLE(0),

    /** Best-effort but only the newest is delivered — older arrivals are dropped (no buffering). */
    UNRELIABLE_SEQUENCED(1),

    /** Guaranteed delivery, any order; duplicates are suppressed. */
    RELIABLE_UNORDERED(2),

    /** Guaranteed delivery, in order within this channel (buffers to reconstruct order). */
    RELIABLE_ORDERED(3);

    private final int code;

    DeliveryMode(int code) {
        this.code = code;
    }

    /** The 1-byte wire code for this mode. */
    public int code() {
        return code;
    }

    /** Whether this mode retransmits until acknowledged. */
    public boolean reliable() {
        return this == RELIABLE_UNORDERED || this == RELIABLE_ORDERED;
    }

    /** The mode for a wire code, or throws on an unknown code. */
    public static DeliveryMode fromCode(int code) {
        for (DeliveryMode m : values()) {
            if (m.code == code) {
                return m;
            }
        }
        throw new IllegalArgumentException("Unknown delivery mode code: " + code);
    }
}
