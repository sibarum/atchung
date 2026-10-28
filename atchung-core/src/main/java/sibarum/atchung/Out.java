package sibarum.atchung;

import java.util.Objects;

/**
 * What a {@link Step} sends through: its ledger's owner's outbox, held until the step returns.
 *
 * <p>A send here belongs to the round, as a {@link Sender}'s inside a delivery does: it reaches its subscribers when
 * the round ends. The difference is that a step's sends are its ledger's to take back. If the step throws, everything
 * it sent is discarded along with the state it would have committed, so a transaction either happened, state and sends
 * together, or did not happen at all.
 *
 * <p>An {@code Out} works only while its ledger is running a step, on the thread running it. Kept and used later, or
 * from another thread, it refuses, because by then nothing would take the send back.
 */
public final class Out {

    private final Inbox owner;
    /** The thread running a step of this ledger, or null. Written only by that thread, as {@link Inbox#delivering} is. */
    Thread running;

    Out(Inbox owner) {
        this.owner = owner;
    }

    /** Send {@code event} on {@code topic} when the round ends, unless the step sending it throws. */
    public <E> void send(Topic<E> topic, E event) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(event, "event");
        if (running != Thread.currentThread()) {
            throw new IllegalStateException("an Out sends only from inside one of its ledger's steps, on the thread "
                    + "running it; this one belongs to inbox '" + owner.name + "'");
        }
        owner.post(topic, event);
    }

    @Override
    public String toString() {
        return "Out[" + owner.name + "]";
    }
}
