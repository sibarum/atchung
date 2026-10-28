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
 * <p>A step running in a {@link Chain} also says here how the run goes on: it gives an {@link #result outcome}, or
 * {@link #hold holds} the run for a later step to {@link #resume}. Going on is a send like any other, taken back
 * with the rest if the step throws.
 *
 * <p>An {@code Out} works only while its ledger is running a step, on the thread running it. Kept and used later, or
 * from another thread, it refuses, because by then nothing would take the send back.
 */
public final class Out {

    private final Inbox owner;
    /** The thread running a step of this ledger, or null. Written only by that thread, as {@link Inbox#delivering} is. */
    private Thread running;
    /** The run the running step is in, or null. */
    private Chain.Run run;
    /** The outcome the running step gave, or null. Read by the ledger once the step returns. */
    Object outcome;
    /** Whether the running step has given an outcome or held its run. */
    private boolean went;

    Out(Inbox owner) {
        this.owner = owner;
    }

    void begin(Chain.Run run) {
        this.running = Thread.currentThread();
        this.run = run;
    }

    void end() {
        running = null;
        run = null;
        outcome = null;
        went = false;
    }

    /** Send {@code event} on {@code topic} when the round ends, unless the step sending it throws. */
    public <E> void send(Topic<E> topic, E event) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(event, "event");
        check();
        owner.post(topic, event);
    }

    /**
     * Give this step's outcome: in a chain, the next step's arguments if it is of the type the next link takes, else
     * the end of the run. Outside a chain there is no next step, and the outcome goes nowhere.
     *
     * @throws IllegalStateException if this step has already given an outcome or held its run: a step goes on once
     */
    public void result(Object event) {
        Objects.requireNonNull(event, "event");
        check();
        once();
        if (run != null) {
            outcome = event;
        }
    }

    /**
     * Hold the run this step is in, to be {@link #resume resumed} by a later step: for a step that waits on a person
     * or a job. Keep the {@link Chain.Resume} in the ledger's state.
     *
     * @throws IllegalStateException if this step is not running in a chain, or has already given an outcome
     */
    public Chain.Resume hold() {
        check();
        if (run == null) {
            throw new IllegalStateException("only a step running in a chain has a run to hold");
        }
        once();
        return new Chain.Resume(run);
    }

    /**
     * Carry a held run on, with {@code event} as the outcome of the step that held it. Sent with this step's other
     * sends, and taken back with them if this step throws.
     */
    public void resume(Chain.Resume held, Object event) {
        Objects.requireNonNull(held, "held");
        Objects.requireNonNull(event, "event");
        check();
        held.run.chain().advance(held.run, event, owner);
    }

    /** The id of the chain run this step is in, the same in every step of it; 0 when it is not in one. */
    public long run() {
        check();
        return run == null ? 0 : run.id();
    }

    private void check() {
        if (running != Thread.currentThread()) {
            throw new IllegalStateException("an Out sends only from inside one of its ledger's steps, on the thread "
                    + "running it; this one belongs to inbox '" + owner.name + "'");
        }
    }

    private void once() {
        if (went) {
            throw new IllegalStateException("a step gives one outcome or holds its run, once");
        }
        went = true;
    }

    @Override
    public String toString() {
        return "Out[" + owner.name + "]";
    }
}
