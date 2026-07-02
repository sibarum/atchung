package sibarum.elektro.queue.netcode.sim;

/**
 * Impairment settings for a {@link SimTransport} link.
 *
 * <p>Each outbound frame is delayed by {@code latencyMillis} plus a uniform jitter in
 * {@code [-jitterMillis, +jitterMillis]} (reordering falls out naturally when jitter exceeds the
 * inter-packet gap), dropped with probability {@code lossProbability}, and sent twice with
 * probability {@code duplicateProbability}. {@code seed} makes the loss/duplicate/jitter draws
 * reproducible across runs.
 *
 * @param latencyMillis        base one-way delay added to every frame (>= 0)
 * @param jitterMillis         +/- uniform variation around the base delay (>= 0)
 * @param lossProbability      chance in [0,1] a frame is dropped outright
 * @param duplicateProbability chance in [0,1] a delivered frame is also duplicated
 * @param seed                 RNG seed for reproducible impairment
 */
public record SimParams(
        long latencyMillis, long jitterMillis, double lossProbability, double duplicateProbability, long seed) {

    public SimParams {
        if (latencyMillis < 0 || jitterMillis < 0) {
            throw new IllegalArgumentException("latency/jitter must be >= 0");
        }
        if (lossProbability < 0 || lossProbability > 1 || duplicateProbability < 0 || duplicateProbability > 1) {
            throw new IllegalArgumentException("probabilities must be in [0,1]");
        }
    }

    /** A perfect link: no delay, no loss, no duplication. */
    public static SimParams perfect() {
        return new SimParams(0, 0, 0.0, 0.0, 0L);
    }

    /** A link with fixed one-way {@code latencyMillis} and no loss. */
    public static SimParams latency(long latencyMillis) {
        return new SimParams(latencyMillis, 0, 0.0, 0.0, 0L);
    }

    /** A link that drops a fraction of frames, otherwise instant. */
    public static SimParams lossy(double lossProbability, long seed) {
        return new SimParams(0, 0, lossProbability, 0.0, seed);
    }
}
