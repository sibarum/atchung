package sibarum.elektro.queue.netcode.sim;

/**
 * Counters for what a {@link SimTransport} did to outbound frames: how many it was asked to send,
 * how many it dropped, and how many duplicates it injected.
 */
public record SimStats(long offered, long dropped, long duplicated) {

    /** Frames actually forwarded to the wire (each once), i.e. {@code offered - dropped}. */
    public long delivered() {
        return offered - dropped;
    }
}
