package sibarum.atchung;

/**
 * A live registration. Closing it stops delivery to its subscriber and releases the registration;
 * closing is idempotent.
 */
public interface Subscription extends AutoCloseable {

    /** @return whether this subscription is still delivering. */
    boolean isActive();

    @Override
    void close();
}
