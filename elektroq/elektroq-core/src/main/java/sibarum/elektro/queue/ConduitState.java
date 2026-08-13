package sibarum.elektro.queue;

/**
 * Lifecycle states of a {@link Conduit}.
 *
 * <pre>
 *   NEW --start()--> STARTING --> OPEN --close()--> CLOSING --> CLOSED
 *                        \                              /
 *                         \--------> FAILED <----------/
 * </pre>
 */
public enum ConduitState {

    /** Created but not yet started; no transport activity. */
    NEW,

    /** {@link Conduit#start()} called; transport is coming up. */
    STARTING,

    /** Ready to emit and react. */
    OPEN,

    /** {@link Conduit#close()} called; draining and shutting down. */
    CLOSING,

    /** Fully shut down; terminal. */
    CLOSED,

    /** Startup or transport failure; terminal. */
    FAILED
}
