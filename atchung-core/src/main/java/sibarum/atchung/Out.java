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
    /** The undo group of the first run the running step resumed, or 0: a step that answers a held run is part of it. */
    private long resumed;
    /** The run the running step held, kept by the ledger once the step returns, or null. */
    Waits.Hold held;
    /** The holds the running step resumed, let go by the ledger once the step returns. */
    private Waits.Hold[] resumes = new Waits.Hold[2];
    private int resumeCount;

    Out(Inbox owner) {
        this.owner = owner;
    }

    void begin(Chain.Run run) {
        this.running = Thread.currentThread();
        this.run = run;
    }

    /** The step returned: keep the bound of the run it held, and let go of the ones it resumed. */
    void commit() {
        if (held == null && resumeCount == 0) {
            return;
        }
        Waits waits = Waits.of(owner);
        if (held != null) {
            waits.held(held);
        }
        for (int i = 0; i < resumeCount; i++) {
            waits.resumed(resumes[i]);
        }
    }

    void end() {
        running = null;
        run = null;
        outcome = null;
        went = false;
        resumed = 0;
        held = null;
        for (int i = 0; i < resumeCount; i++) {
            resumes[i] = null;
        }
        resumeCount = 0;
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
     * Hold the run this step is in, to be {@link #resume resumed} by a later step on this ledger's inbox: for a step
     * that waits on a job, or on anything a chain's waits cannot say. Keep the {@link Chain.Resume} in the ledger's
     * state.
     *
     * <p><b>The hold is bounded</b>, as every wait is: by the link's {@code within(...)}, else the chain's default, else
     * the tick's. When the bound runs out first the run goes on with a {@link TimedOut}, and a resume after that is
     * refused. A link that may wait forever says {@code forever()}.
     *
     * @throws IllegalStateException if this step is not running in a chain, or has already given an outcome
     */
    public Chain.Resume hold() {
        return hold(false);
    }

    /**
     * Hold the run until the person answers, through the framework's dialogs, which guarantee an answer (closing one
     * unanswered answers too): bounded by the dialog rather than a time, because a person may take an hour
     * (vexelray-framework's {@code docs/plans/chains.md}, ruling 4). Otherwise as {@link #hold()}.
     */
    public Chain.Resume holdForPerson() {
        return hold(true);
    }

    private Chain.Resume hold(boolean person) {
        check();
        if (run == null) {
            throw new IllegalStateException("only a step running in a chain has a run to hold");
        }
        once();
        held = new Waits.Hold(run, owner, person);
        return new Chain.Resume(run, held);
    }

    /**
     * Carry a held run on, with {@code event} as the outcome of the step that held it. Sent with this step's other
     * sends, and taken back with them if this step throws. A run goes on once: one already resumed, or whose bound
     * ran out, is refused and traced, not taken twice.
     *
     * @throws IllegalStateException if it is resumed on another inbox than the one whose step held it
     */
    public void resume(Chain.Resume held, Object event) {
        Objects.requireNonNull(held, "held");
        Objects.requireNonNull(event, "event");
        check();
        Waits.Hold hold = held.hold;
        if (hold.inbox != owner) {
            throw new IllegalStateException("a held run is resumed on the inbox whose step held it, '" + hold.inbox.name
                    + "', and this step runs on '" + owner.name + "': keep the Resume in that ledger's state");
        }
        if (hold.done || resuming(hold)) {
            if (sibarum.probe.Probe.ON) {
                sibarum.probe.Probe.mark(sibarum.probe.Lane.BUS, "chain resumed late", held.toString());
            }
            return;
        }
        if (resumed == 0) {
            resumed = held.run.life().group;
        }
        if (resumeCount == resumes.length) {
            resumes = java.util.Arrays.copyOf(resumes, resumeCount * 2);
        }
        resumes[resumeCount++] = hold;
        held.run.chain().advance(held.run, event, owner);
    }

    private boolean resuming(Waits.Hold hold) {
        for (int i = 0; i < resumeCount; i++) {
            if (resumes[i] == hold) {
                return true;
            }
        }
        return false;
    }

    /** The id of the chain run this step is in, the same in every step of it; 0 when it is not in one. */
    public long run() {
        check();
        return run == null ? 0 : run.id();
    }

    /**
     * What the running step's change is undone with: its run's group (the root run's id, so a child run's changes go
     * with its parent's), or the group of the run it resumed, or 0 for a step on its own.
     */
    long group() {
        return run != null ? run.life().group : resumed;
    }

    /** Queue {@code work} for the coordinator, between this round and the next. Taken back if the step throws. */
    void between(Inbox.Between work) {
        check();
        owner.queueBetween(work);
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
