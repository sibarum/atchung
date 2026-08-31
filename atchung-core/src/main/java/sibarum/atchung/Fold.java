package sibarum.atchung;

/**
 * Which <em>cell</em> an event writes, for a pumped mailbox that folds. An event that names a cell supersedes
 * whatever is queued for that same cell; an event that names none is an edge and queues on its own.
 *
 * <p><b>Why a mailbox would fold at all.</b> Some channels carry writes rather than occurrences. Setting a
 * label's text to "a" and then to "b" before the consumer has run is not two events — it is two writes to one
 * cell, and only the second is observable. Queueing both costs a slot, a delivery and a redundant application,
 * and does so in proportion to how fast the producer runs rather than to how much actually changed.
 *
 * <p>{@link Backpressure#COALESCE_LATEST} already says this for a channel where <em>everything</em> is one
 * cell — pointer position, window size. This is the same idea for a channel where only some of the traffic is,
 * and where the rest must not be touched: a tree's property writes fold, its structural edits do not.
 *
 * <p><b>The condition that makes folding identity rather than approximation</b> is on the consumer, and it must
 * be checked before using this: <em>nothing may observe the consumer's state between a publish and a drain.</em>
 * Where that holds, everything published between two drains is one transaction whose only observable is the
 * state at its end, and dropping a superseded write cannot be detected by anything, even in principle. Where it
 * does not hold — where somebody can see the intermediate values — folding loses information that was being
 * read, and the channel is not a candidate.
 *
 * <p><b>Superseding moves the event to the back of the queue</b>, to the position of the write that survived,
 * rather than overwriting the earlier one in place. That keeps the queue in arrival order for the write that
 * wins, which is what makes folding invisible next to the edges: an event folded past an edge that would have
 * invalidated it (a write to a node that has since been destroyed) ends up after that edge, exactly where the
 * unfolded sequence would have put it.
 */
@FunctionalInterface
public interface Fold<T> {

    /**
     * The cell {@code event} writes — any value with sensible {@code equals}/{@code hashCode} — or {@code null}
     * if this event is an edge that supersedes nothing and is superseded by nothing.
     *
     * <p>Called on the publisher's thread, once per event, inside the mailbox lock: keep it cheap and pure.
     */
    Object cell(T event);
}
