package sibarum.atchung;

/**
 * What a <em>pumped</em> subscriber's bounded mailbox does when it is full at publish time. The
 * publisher must never be stalled by a slow consumer on the realtime path, so the default policies
 * shed load rather than block.
 */
public enum Backpressure {

    /** Evict the oldest queued event to make room for the newest. Good default for event streams. */
    DROP_OLDEST,

    /** Drop the incoming event, keeping what is already queued. */
    DROP_NEWEST,

    /**
     * Keep only the most recent event — the mailbox holds at most one. Ideal for state-like signals
     * (pointer position, window size) where only the latest value matters and intermediates are noise.
     */
    COALESCE_LATEST,

    /**
     * Block the publisher until the consumer drains space. The only policy that applies real
     * backpressure upstream; use it off the realtime path, never for high-frequency input.
     */
    BLOCK
}
