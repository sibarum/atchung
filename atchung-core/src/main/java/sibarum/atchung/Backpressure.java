package sibarum.atchung;

/**
 * What a <em>pumped</em> subscriber's bounded mailbox does when it is full at publish time.
 *
 * <p><b>Choose {@link #FAIL} unless you can say why loss is harmless on this channel.</b> The other policies
 * lose events, and a lost event is not an error anywhere — it is an absence, which reads downstream as an
 * input the user never made, a frame that never changed, a command that never arrived. The bug is then looked
 * for in the consumer, which is working perfectly. Losing data quietly is a decision, and a decision has to be
 * made deliberately and written down at the subscription that makes it.
 *
 * <p>The test is per <em>channel</em>, and it is about the payload's loss class:
 *
 * <ul>
 *   <li><b>A sample</b> — pointer position, window size, a clock reading. The next one supersedes this one,
 *       so dropping it costs nothing that could still have been drawn. {@link #COALESCE_LATEST}.</li>
 *   <li><b>An edge</b> — a keystroke, a button press, a command, a tree mutation. Nothing supersedes it and
 *       nothing downstream can reconstruct it. {@link #FAIL}, or {@link #BLOCK} where the publisher can
 *       safely wait.</li>
 * </ul>
 *
 * <p>A channel carrying <em>both</em> classes cannot be given a correct policy — every choice is wrong for
 * half the traffic. That is not a policy problem to be solved here; it is a signal to split the channel.
 */
public enum Backpressure {

    /**
     * Report the overflow and stop: the process-wide {@link Fatal} policy runs (by default halting the
     * process), then a {@link MailboxOverflow} is thrown at the publish site.
     *
     * <p><b>The right choice for anything that must not be lost</b>, and the one to reach for by default. It
     * is louder than the alternatives by design: an overflow means the consumer is not keeping up, and the
     * only options are to stop, to block the publisher, or to produce wrong answers quietly. A crash naming
     * the topic, the capacity and the delivery count is the cheapest of the three to diagnose and the only
     * one of the three that cannot be mistaken for working.
     *
     * <p>Unlike {@link #BLOCK} it cannot deadlock, so it is available where the publisher and the drain share
     * a thread — which is exactly where {@code BLOCK} is unusable and lossy policies used to be the only
     * thing left.
     */
    FAIL,

    /**
     * Evict the oldest queued event to make room for the newest.
     *
     * <p><b>Sheds the events that have been waiting longest</b>, which on a mixed channel means the
     * keystrokes rather than the pointer motion that displaced them. Correct only where the newest event is
     * genuinely the most valuable and the old ones are stale — a progress reading, a frame-time sample.
     * Nothing reports how many were dropped, so a subscription using this is choosing to lose data
     * unobservably.
     */
    DROP_OLDEST,

    /**
     * Drop the incoming event, keeping what is already queued. Preserves the front of a burst, which is the
     * right end when what matters is the sequence that started rather than the state that ended.
     */
    DROP_NEWEST,

    /**
     * Keep only the most recent event — the mailbox holds at most one. The correct policy for state-like
     * signals (pointer position, window size) where only the latest value matters and the intermediates are
     * noise. This is the one lossy policy that loses nothing, because on such a channel the dropped events
     * carry no information the survivor does not.
     */
    COALESCE_LATEST,

    /**
     * Block the publisher until the consumer drains space. The only policy that applies real backpressure
     * upstream, and the only one that neither loses an event nor stops the process.
     *
     * <p><b>Deadlocks if the publisher and the drain are the same thread</b> — a frame loop that publishes
     * into its own pump waits on itself for ever. Use it where the producer is genuinely a different thread
     * that can afford to wait; use {@link #FAIL} where it is not.
     */
    BLOCK
}
