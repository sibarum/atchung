package sibarum.elektro.queue.netcode.reliable;

/**
 * Wrap-safe comparison for 16-bit packet sequence numbers.
 *
 * <p>Sequence numbers count up and wrap at 65536, so a naive {@code a > b} is wrong across the wrap
 * (sequence 0 is <em>newer</em> than 65535, not older). {@link #moreRecent} compares within a
 * half-range window, the standard trick: {@code a} is more recent than {@code b} when it is ahead by
 * less than half the space, treating the space as circular. Only ever used to compare sequences
 * within a small recent window, so the half-range test is unambiguous.
 */
public final class SequenceMath {

    private static final int HALF = 0x8000; // 32768

    private SequenceMath() {}

    /** {@code true} if 16-bit sequence {@code a} is more recent than {@code b} (wrap-aware). */
    public static boolean moreRecent(int a, int b) {
        return ((a > b) && (a - b <= HALF)) || ((b > a) && (b - a > HALF));
    }
}
