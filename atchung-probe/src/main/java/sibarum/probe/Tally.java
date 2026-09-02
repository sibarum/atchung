package sibarum.probe;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Everything remembered about one named thing, accumulated lock-free from every thread that touches it.
 *
 * <p>Two shapes share this class because the report wants them side by side. A <b>span</b> tally carries
 * durations and answers "how long"; a <b>counter</b> tally carries a running sum and answers "how many". A
 * frame is a span, a published event is a counter, and the interesting question — is the cost per event or
 * the number of events? — is one you can only ask if both are in front of you.
 *
 * <h2>Why a histogram rather than the samples</h2>
 *
 * The number that matters in a frame budget is not the mean. A loop that averages 4 ms and spikes to 90 ms
 * once a second is a visibly broken loop with a healthy average, and every stutter complaint this facility
 * exists to answer is that shape. So durations go into a log-scale histogram — four buckets per octave, 256
 * buckets total, about 12% resolution — which gives percentiles for a fixed 2 KB per name and no allocation
 * per sample. Keeping the raw samples would be exact, and would allocate unboundedly in exactly the run that
 * is already suspected of leaking.
 */
final class Tally {

    /** Four sub-buckets per power of two: ~12% relative error, which is finer than any decision made on it. */
    private static final int SUB_BITS = 2;
    private static final int SUBS = 1 << SUB_BITS;
    private static final int BUCKETS = 64 * SUBS;

    final String name;
    final Lane lane;
    final boolean span;

    /**
     * Whether this span is the process deliberately doing nothing - a frame loop parked on an empty event
     * queue, a worker waiting on a gate.
     *
     * <p>Such a span is still worth measuring: a wait total that is small next to the run total is the
     * signature of a loop that spins instead of sleeping. But it must never trip the slow-span report, which
     * exists to name frames that ran long. A render-on-demand application idles for half a second at a time
     * by design, and a "slow" line for every one of those buries the one frame that really was late.
     */
    final boolean idle;

    private final LongAdder count = new LongAdder();
    private final LongAdder total = new LongAdder();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLongArray hist;

    /**
     * Time inside this span that was not inside a nested span: its <b>self</b> time.
     *
     * <p>The number that localises a cost. A frame span of 40 ms tells you the frame was late; a frame span
     * of 40 ms whose self time is 1 ms tells you it was late in something it called, and the child holding
     * 38 ms of self time is the answer. Only a sum and a peak are kept — percentiles on self time have never
     * been the question, and two counters cost nothing.
     */
    private final LongAdder selfTotal = new LongAdder();
    private final AtomicLong selfMax = new AtomicLong(Long.MIN_VALUE);

    Tally(Lane lane, String name, boolean span, boolean idle) {
        this.lane = lane;
        this.name = name;
        this.span = span;
        this.idle = idle;
        this.hist = span ? new AtomicLongArray(BUCKETS) : null;
    }

    /** Record one span of {@code nanos}. */
    void record(long nanos) {
        count.increment();
        total.add(nanos);
        bumpMax(max, nanos);
        bumpMin(nanos);
        hist.incrementAndGet(bucket(nanos));
    }

    /** Record the self time of a span already passed to {@link #record}. */
    void recordSelf(long nanos) {
        selfTotal.add(nanos);
        bumpMax(selfMax, nanos);
    }

    /** Add {@code n} to a counter, as one observation. */
    void bump(long n) {
        count.increment();
        total.add(n);
        bumpMax(max, n);
        bumpMin(n);
    }

    private static void bumpMax(AtomicLong ceiling, long v) {
        long seen;
        while (v > (seen = ceiling.get()) && !ceiling.compareAndSet(seen, v)) {
            // Another thread moved the ceiling between the read and the write; re-read and try again.
        }
    }

    private void bumpMin(long v) {
        long seen;
        while (v < (seen = min.get()) && !min.compareAndSet(seen, v)) {
            // As above.
        }
    }

    long count() {
        return count.sum();
    }

    long total() {
        return total.sum();
    }

    long selfTotal() {
        return selfTotal.sum();
    }

    long max() {
        long v = max.get();
        return v == Long.MIN_VALUE ? 0 : v;
    }

    long selfMax() {
        long v = selfMax.get();
        return v == Long.MIN_VALUE ? 0 : v;
    }

    long min() {
        long v = min.get();
        return v == Long.MAX_VALUE ? 0 : v;
    }

    long mean() {
        long n = count();
        return n == 0 ? 0 : total() / n;
    }

    /**
     * The value at {@code q} (0..1) of the recorded durations, to within one bucket.
     *
     * <p>Reported as the bucket's <em>lower</em> bound rather than its midpoint: a percentile quoted low reads
     * as "at least this bad", which is the safe direction to be wrong in when the number is about to be
     * compared against a 16.6 ms budget.
     */
    long percentile(double q) {
        if (!span) {
            return 0;
        }
        long n = count();
        if (n == 0) {
            return 0;
        }
        long target = (long) Math.ceil(q * n);
        long seen = 0;
        for (int i = 0; i < BUCKETS; i++) {
            seen += hist.get(i);
            if (seen >= target) {
                return lowerBound(i);
            }
        }
        return max();
    }

    /** Which bucket {@code v} lands in: the octave, then two bits of mantissa below it. */
    private static int bucket(long v) {
        long x = v < 1 ? 1 : v;
        int octave = 63 - Long.numberOfLeadingZeros(x);
        int sub = octave < SUB_BITS ? 0 : (int) ((x >>> (octave - SUB_BITS)) & (SUBS - 1));
        return octave * SUBS + sub;
    }

    /** The smallest value that lands in bucket {@code i} — the inverse of {@link #bucket}. */
    private static long lowerBound(int i) {
        int octave = i / SUBS;
        int sub = i % SUBS;
        long base = 1L << octave;
        return octave < SUB_BITS ? base : base + ((long) sub << (octave - SUB_BITS));
    }
}
