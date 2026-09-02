package sibarum.atchung;

/**
 * A typed, pre-declared commit command — the only way to change a {@link State}. Obtained from
 * {@link State.Builder#mutation(String, Mutation)} and applied via {@link State#commit(Committer, Object)}.
 *
 * <p>Commit commands are declared ahead of time, not assembled ad-hoc: a {@code State} accepts only
 * the committers it was built with. The {@code name} is the stable identity a future replication
 * bridge uses to ship a mutation to a replica (op-based) — it never affects local dispatch.
 *
 * @param <T> the state type
 * @param <P> the payload type
 */
public final class Committer<T, P> {

    private final String name;
    private final Mutation<T, P> mutation;

    /**
     * The probe's name for this committer's retry counter, built once here and null when not profiling.
     *
     * <p>{@link State#commit} is a CAS loop, so a contended state does its work twice and reports the same
     * commit once. Retries are the only place that shows, and a retry count that rivals the commit count is
     * two producers on a state the design says has one — which is a correctness finding arriving as a
     * performance one, and worth a name of its own rather than an aggregate.
     */
    final String retryName;

    Committer(String name, Mutation<T, P> mutation) {
        this.name = name;
        this.mutation = mutation;
        this.retryName = sibarum.probe.Probe.ON ? name + " retries" : null;
    }

    /** @return the stable declaration name (for diagnostics and replication identity). */
    public String name() {
        return name;
    }

    T applyTo(T current, P payload) {
        return mutation.apply(current, payload);
    }

    @Override
    public String toString() {
        return "Committer[" + name + "]";
    }
}
