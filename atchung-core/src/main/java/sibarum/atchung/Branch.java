package sibarum.atchung;

import java.time.Duration;
import java.util.Objects;

/**
 * What a wait in a {@link Chain} waits for: a message on a topic, or a whole chain. A branch that is a chain makes
 * flows compose: an {@link Chain.Builder#all ALL} over two is two flows that join, an {@link Chain.Builder#any ANY}
 * over two is a race between flows, and one chain as a branch with a bound is a deadline on the whole of it
 * (vexelray-framework's {@code docs/plans/chains.md}, ruling 2).
 *
 * @param <X> what the branch arrives with: the message, or what the chain ends with
 */
public abstract class Branch<X> {

    private Branch() {
    }

    /** A message on {@code topic}: one that belongs to the waiting run, or to none (ruling 9). */
    public static <X> Branch<X> of(Topic<X> topic) {
        return new Message<>(Objects.requireNonNull(topic, "topic"));
    }

    /** What the branch is called in a {@link Missing} or a trace. */
    abstract String label();

    /** Whether {@code value} is what this branch waits for, as an {@link Chain.Builder#all ALL} counts it. */
    abstract boolean takes(Object value);

    /** A message branch. */
    static final class Message<X> extends Branch<X> {
        final Topic<X> topic;

        Message(Topic<X> topic) {
            this.topic = topic;
        }

        @Override
        String label() {
            return topic.name();
        }

        @Override
        boolean takes(Object value) {
            return topic.payloadType().isInstance(value);
        }
    }

    /**
     * A chain used as a branch, from {@link Chain#asBranch()}. It starts with the outcome the wait was entered with,
     * runs as a child of the waiting run, and arrives with what it ends with. Its own bound is a deadline on the whole
     * of it; without one, each of its waits keeps its own, so it ends (ruling 4).
     */
    public static final class OfChain<X> extends Branch<X> {
        final Chain<?, ?, X> chain;
        final Bound bound;

        OfChain(Chain<?, ?, X> chain, Bound bound) {
            this.chain = chain;
            this.bound = bound;
        }

        /** A deadline on the whole chain: when it runs out the chain is cut and the branch arrives with a {@link TimedOut}. */
        public OfChain<X> within(Duration deadline) {
            return new OfChain<>(chain, Bound.of(deadline));
        }

        /** No deadline on the whole chain, which is also what it has unless {@link #within} says otherwise. */
        public OfChain<X> forever() {
            return new OfChain<>(chain, null);
        }

        @Override
        String label() {
            return chain.name();
        }

        @Override
        boolean takes(Object value) {
            return chain.ends().isInstance(value);
        }
    }
}
