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

    Committer(String name, Mutation<T, P> mutation) {
        this.name = name;
        this.mutation = mutation;
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
