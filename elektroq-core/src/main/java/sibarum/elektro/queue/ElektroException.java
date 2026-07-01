package sibarum.elektro.queue;

/**
 * Unchecked base type for all elektro-Q runtime failures &mdash; malformed frames,
 * unknown message ids, codec errors, and conduit/transport faults.
 *
 * <p>elektro-Q favours unchecked exceptions so that emit/react call sites stay clean
 * and so that failures propagate naturally through {@code CompletionStage} pipelines.
 */
public class ElektroException extends RuntimeException {

    public ElektroException(String message) {
        super(message);
    }

    public ElektroException(String message, Throwable cause) {
        super(message, cause);
    }
}
