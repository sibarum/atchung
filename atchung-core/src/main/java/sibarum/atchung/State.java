package sibarum.atchung;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import sibarum.probe.Lane;
import sibarum.probe.Probe;
import sibarum.probe.Zone;

/**
 * A synchronized state cell: a single producer owns a value; consumers read coherent, immutable,
 * versioned snapshots. The alternative broadcast shape to {@link Topic} events — instead of "what
 * happened," it holds "what is true now."
 *
 * <p><b>Producer.</b> The value changes only through pre-declared {@link Committer commit commands}
 * (Vuex-style): {@code state.commit(MOVE, delta)}. Each commit atomically produces a new immutable
 * {@link Versioned} with the next version number. Atomicity is per-state (one producer); there is no
 * cross-state transaction.
 *
 * <p><b>Consumers.</b>
 * <ul>
 *   <li>poll — {@link #current()} / {@link #value()} / {@link #version()} (read as often or rarely
 *       as you like; there is no mailbox to overflow);</li>
 *   <li>react — {@link #onCommit(StateListener)} (notified per new version; pausable/lossy);</li>
 *   <li>block — {@link #await(long)} (park until a newer version exists).</li>
 * </ul>
 *
 * <p><b>Threadsafe by construction, not obstruction:</b> reads are lock-free (an immutable snapshot
 * behind an {@link AtomicReference}); commit is a lock-free CAS. No mutex sits on the data path.
 *
 * <p>Bounded history ({@link Builder#history}) keeps the last N versions (and/or a TTL) for
 * {@link #at(long)} lookups; the depth bound guarantees no unbounded growth.
 *
 * @param <T> the state type (should be immutable)
 */
public final class State<T> {

    private final AtomicReference<Versioned<T>> current;
    private final Set<Committer<?, ?>> declared;
    private final CopyOnWriteArrayList<Reg<T>> listeners = new CopyOnWriteArrayList<>();

    private final int maxDepth;
    private final Duration ttl;
    private final ConcurrentSkipListMap<Long, Versioned<T>> history; // version -> snapshot; null if no history

    private State(Builder<T> builder) {
        this.declared = new HashSet<>(builder.committers);
        this.maxDepth = builder.maxDepth;
        this.ttl = builder.ttl;
        this.history = maxDepth > 0 ? new ConcurrentSkipListMap<>() : null;
        Versioned<T> initial = new Versioned<>(builder.initial, 0L, System.nanoTime());
        this.current = new AtomicReference<>(initial);
        if (history != null) {
            history.put(0L, initial);
        }
    }

    /** Begin building a state with its initial value. */
    public static <T> Builder<T> of(T initial) {
        return new Builder<>(Objects.requireNonNull(initial, "initial"));
    }

    // --- Producer ---------------------------------------------------------

    /**
     * Apply a declared mutation atomically, producing the next version. Lock-free (CAS); the mutation
     * may be retried under contention, so it must be pure.
     *
     * @throws IllegalArgumentException if {@code committer} was not declared on this state
     */
    public <P> void commit(Committer<T, P> committer, P payload) {
        apply(committer, payload, false);
    }

    /**
     * Like {@link #commit}, but a mutation that produces a value {@link Objects#equals equal} to the current
     * one commits nothing: no new version, no listener woken. Saves every caller the read-compare-commit it
     * would otherwise write, and does it correctly, since the comparison is made against the value the CAS
     * is about to replace rather than a read taken before it.
     *
     * @return {@code true} if a new version was committed, {@code false} if the value did not change
     * @throws IllegalArgumentException if {@code committer} was not declared on this state
     */
    public <P> boolean commitIfChanged(Committer<T, P> committer, P payload) {
        return apply(committer, payload, true);
    }

    private <P> boolean apply(Committer<T, P> committer, P payload, boolean onlyIfChanged) {
        Objects.requireNonNull(committer, "committer");
        if (!declared.contains(committer)) {
            throw new IllegalArgumentException("mutation not declared on this State: " + committer.name());
        }
        // The span covers the CAS loop and the listener fan-out both, because from the committing thread's
        // point of view they are one call and one cost. Listeners run on this thread.
        try (Zone z = Probe.zone(Lane.STATE, committer.name())) {
            Versioned<T> next;
            while (true) {
                Versioned<T> prev = current.get();
                T value = committer.applyTo(prev.value(), payload);
                if (onlyIfChanged && Objects.equals(value, prev.value())) {
                    return false;
                }
                next = new Versioned<>(value, prev.version() + 1, System.nanoTime());
                if (current.compareAndSet(prev, next)) {
                    break;
                }
                Probe.count(Lane.STATE, committer.retryName);
            }
            record(next);
            for (Reg<T> reg : listeners) {
                reg.deliver(next);
            }
            return true;
        }
    }

    // --- Consumers: poll --------------------------------------------------

    /** @return the latest coherent snapshot (lock-free read). */
    public Versioned<T> current() {
        return current.get();
    }

    /** @return the latest value. */
    public T value() {
        return current.get().value();
    }

    /** @return the current version number. */
    public long version() {
        return current.get().version();
    }

    // --- Consumers: react -------------------------------------------------

    /** Register a reactive listener, fired on the committing thread per new version. Pausable/lossy. */
    public Subscription onCommit(StateListener<T> listener) {
        Reg<T> reg = new Reg<>(Objects.requireNonNull(listener, "listener"));
        listeners.add(reg);
        return new StateSubscription(reg);
    }

    /**
     * Register a listener that is delivered in version order, one at a time, and always ends on the newest.
     *
     * <p>{@link #onCommit} fires on the committing thread <em>after</em> that thread's CAS, so two threads
     * committing together can deliver version 6 then 5, or at the same instant. A listener that redraws from
     * the snapshot would then finish on a stale one, or run twice at once. Here delivery is serialised, and a
     * snapshot no newer than the last one delivered is dropped. Nothing is lost by that: the newer snapshot
     * that overtook it is the whole state, not a delta.
     *
     * <p>The lock is on the <em>listener's</em> path, not the data path: commits and reads stay lock-free,
     * but a committing thread may wait for another's delivery to finish. Keep the listener short. Handing off
     * to an executor rebuilds the ordering problem; carry {@link Versioned#version()} across and drop what
     * is older.
     *
     * <p>Pausable and closeable like any {@link Subscription}.
     */
    public Subscription onCommitLatest(StateListener<T> listener) {
        Objects.requireNonNull(listener, "listener");
        Object gate = new Object();
        long[] delivered = {-1};
        return onCommit(snap -> {
            synchronized (gate) {
                if (snap.version() <= delivered[0]) {
                    return;
                }
                delivered[0] = snap.version();
                listener.onCommit(snap);
            }
        });
    }

    // --- Consumers: block -------------------------------------------------

    /**
     * Block until a version strictly greater than {@code afterVersion} exists, then return it. Returns
     * immediately if already ahead. This is the only method that parks the caller — and only the
     * caller, by its own choice.
     */
    public Versioned<T> await(long afterVersion) throws InterruptedException {
        Versioned<T> now = current.get();
        if (now.version() > afterVersion) {
            return now;
        }
        CompletableFuture<Versioned<T>> future = new CompletableFuture<>();
        try (Subscription sub = onCommit(snap -> {
            if (snap.version() > afterVersion) {
                future.complete(snap);
            }
        })) {
            // Re-check after subscribing so a commit racing the subscribe is not missed.
            Versioned<T> recheck = current.get();
            if (recheck.version() > afterVersion) {
                return recheck;
            }
            try {
                return future.get();
            } catch (ExecutionException e) {
                throw new IllegalStateException("state listener failed", e.getCause());
            }
        }
    }

    // --- History ----------------------------------------------------------

    /**
     * @return the snapshot at {@code version} if still retained in bounded history, else empty
     *         (never kept, aged out by TTL, or evicted by depth).
     */
    public Optional<Versioned<T>> at(long version) {
        if (history == null) {
            return Optional.empty();
        }
        Versioned<T> snap = history.get(version);
        if (snap == null) {
            return Optional.empty();
        }
        if (ttl != null && snap.timestampNanos() < System.nanoTime() - ttl.toNanos()) {
            return Optional.empty();
        }
        return Optional.of(snap);
    }

    private void record(Versioned<T> snap) {
        if (history == null) {
            return;
        }
        history.put(snap.version(), snap);
        // Version order == time order, so evicting from the front satisfies both depth and TTL.
        while (history.size() > maxDepth) {
            history.pollFirstEntry();
        }
        if (ttl != null) {
            long cutoff = snap.timestampNanos() - ttl.toNanos();
            Map.Entry<Long, Versioned<T>> first;
            while ((first = history.firstEntry()) != null && first.getValue().timestampNanos() < cutoff) {
                history.pollFirstEntry();
            }
        }
    }

    /** @return the names of the mutations declared on this state (for diagnostics / a replication bridge). */
    public Set<String> mutationNames() {
        Set<String> names = new HashSet<>();
        for (Committer<?, ?> c : declared) {
            names.add(c.name());
        }
        return names;
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T> {
        private final T initial;
        private final List<Committer<T, ?>> committers = new ArrayList<>();
        private int maxDepth;
        private Duration ttl;

        private Builder(T initial) {
            this.initial = initial;
        }

        /** Declare a typed commit command. Hold the returned handle and commit with it. */
        public <P> Committer<T, P> mutation(String name, Mutation<T, P> mutation) {
            Committer<T, P> committer = new Committer<>(
                    Objects.requireNonNull(name, "name"), Objects.requireNonNull(mutation, "mutation"));
            committers.add(committer);
            return committer;
        }

        /** Keep the last {@code maxDepth} versions for {@link #at(long)}. 0 (default) keeps no history. */
        public Builder<T> history(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        /** Keep up to {@code maxDepth} versions, dropping any older than {@code ttl}. */
        public Builder<T> history(int maxDepth, Duration ttl) {
            this.maxDepth = maxDepth;
            this.ttl = ttl;
            return this;
        }

        public State<T> build() {
            return new State<>(this);
        }
    }

    // --- Listener registration --------------------------------------------

    private static final class Reg<T> {
        private final StateListener<T> listener;
        private volatile boolean paused;

        Reg(StateListener<T> listener) {
            this.listener = listener;
        }

        void deliver(Versioned<T> snap) {
            if (!paused) {
                listener.onCommit(snap);
            }
        }
    }

    private final class StateSubscription implements Subscription {
        private final Reg<T> reg;
        private volatile boolean active = true;

        StateSubscription(Reg<T> reg) {
            this.reg = reg;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void pause() {
            reg.paused = true;
        }

        @Override
        public void resume() {
            reg.paused = false;
        }

        @Override
        public boolean isPaused() {
            return reg.paused;
        }

        @Override
        public void close() {
            if (!active) {
                return;
            }
            active = false;
            listeners.remove(reg);
        }
    }
}
