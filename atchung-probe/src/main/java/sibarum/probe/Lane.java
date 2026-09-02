package sibarum.probe;

/**
 * A subsystem the probe can be pointed at, and the unit the {@code probe=} switch selects.
 *
 * <p>Lanes exist because "turn on profiling" is almost never the question. The question is whether the
 * frame budget is going on the GPU or on layout, whether a mailbox is backing up or the input backend is
 * spinning — and each of those is answered by one lane's worth of output. Turning on all of them at once
 * produces a wall of text in which the answer is present and unfindable, which is the failure mode this
 * whole facility exists to avoid.
 *
 * <p>The set is deliberately small and deliberately named after layers rather than modules: a lane
 * outlives the module that currently owns the code, and a report that says {@code GPU} tells you where to
 * look without your having to remember which artifact the presenter lives in this month.
 */
public enum Lane {

    /** Atchung: publish, fan-out, mailbox depth, drains. The spine every other lane hangs off. */
    BUS("bus"),

    /** Atchung {@code State<T>}: commits, versions, and the readers waiting on them. */
    STATE("state"),

    /** Kronometer's kernel: handoffs, the timeline, logical time against the wall. */
    TIME("time"),

    /** Kronometer's animation layer: signals, curves, effects, and what re-evaluates per frame. */
    ANIM("anim"),

    /** Tactroller: the OS input backend, event translation, shortcuts, drag and drop. */
    INPUT("input"),

    /** The frame loop itself: one span per frame, plus the budget it chose and the wait it took. */
    FRAME("frame"),

    /** Reconcile, measure, arrange — the tree work between a mutation and a drawable node. */
    LAYOUT("layout"),

    /** Canvas emission, glyph runs, vertex counts: what the frame actually asked the GPU to draw. */
    DRAW("draw"),

    /** VexelRay's Vulkan edge: acquire, submit, present, and every native object's lifetime. */
    GPU("gpu"),

    /** SupirVast: shader compilation, lowering, caching — the costs that look like a hang, once. */
    SHADER("shader"),

    /** The application's own spans. Nothing in the framework publishes here; it is yours. */
    APP("app");

    private final String key;

    /**
     * Whether this lane is selected. Read on every hot-path call, so it is a plain field rather than a set
     * lookup, and it is written exactly once during {@link Probe}'s static initialisation.
     */
    volatile boolean on;

    /**
     * This lane's tallies, by name.
     *
     * <p>Per lane rather than one map for the whole probe, so a call site can key on a bare name it already
     * holds — a topic name, a shred name — instead of a lane-qualified one it would have to build. Composing
     * that key would allocate a string per event on the very paths this is measuring, and a profiler that
     * allocates per event is measuring itself.
     */
    final java.util.concurrent.ConcurrentHashMap<String, Tally> spans = new java.util.concurrent.ConcurrentHashMap<>();
    final java.util.concurrent.ConcurrentHashMap<String, Tally> counts = new java.util.concurrent.ConcurrentHashMap<>();

    Lane(String key) {
        this.key = key;
    }

    /** The name this lane answers to in {@code -Dprobe=...}. Lower case, stable, and not the enum name. */
    public String key() {
        return key;
    }

    /**
     * Whether output for this lane is wanted.
     *
     * <p>Always guard a call site with {@link Probe#ON} <em>first</em> — {@code if (Probe.ON && Lane.GPU.on())}.
     * The constant is what lets the JIT delete the whole expression in a build that is not profiling; reaching
     * the lane check means the field read has already been paid for.
     */
    public boolean on() {
        return on;
    }

    /** The lane with this {@link #key()}, or {@code null}. Case-insensitive; the enum name works too. */
    static Lane byKey(String s) {
        String k = s.trim().toLowerCase(java.util.Locale.ROOT);
        for (Lane lane : values()) {
            if (lane.key.equals(k)) {
                return lane;
            }
        }
        return null;
    }
}
