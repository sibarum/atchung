package sibarum.atchung;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Undo by snapshot: what {@link Ledger.Builder#undoable undoable} ledgers changed, grouped by the cause that changed
 * them, and put back a group at a time, latest first.
 *
 * <pre>{@code
 * Undo undo = Undo.build(tick.inbox("history"), "history.tx", 64, 100);
 * Ledger<Doc> editor = Ledger.of(doc).undoable(undo)...build(...);
 *
 * bus.publish(undo.topic(), undo.undo());      // Ctrl+Z
 * bus.publish(undo.topic(), undo.redo());      // Ctrl+Y
 * }</pre>
 *
 * <ul>
 *   <li><b>A group is a cause.</b> Every change a {@link Chain} run made, on every ledger it reached, is one group, so
 *       one undo takes back a whole close-a-document, not the editor's half of it. A step that resumed a held run is
 *       part of that run. A change made in no run is a group of its own.</li>
 *   <li><b>A snapshot, not an inverse.</b> For each ledger in the group, undo restores the state it had before the
 *       group's first change there; redo restores the state after its last. Nobody writes a reverse.</li>
 *   <li><b>All or nothing, across ledgers.</b> Undo and redo run on the tick's coordinator between rounds, when no step
 *       of any ledger is running, so the check and the restore see one moment. If any ledger in the group does not hold
 *       what the group left (compared by {@code equals}, as a value is), or the group's changes to one ledger were interleaved with
 *       another cause's, nothing is restored and the outcome is {@link Kind#CONFLICT}: a snapshot put back over a
 *       change it did not see would silently lose that change.</li>
 *   <li><b>Latest first.</b> The groups are a stack, and a new change after an undo clears what could be redone, as in
 *       every editor. Taking an older group back out of order would need inverse operations, which this does not
 *       have.</li>
 *   <li><b>What undo does is said on {@link #outcomes()}</b>, one {@link Outcome} per request. A restore is not itself
 *       recorded, so undoing does not add to what can be undone.</li>
 * </ul>
 *
 * <p>An effect (a file written, a process started) is not a ledger's state and is not undone here; the mailbox that
 * does it decides what undoing it means. Why the stack wants this is vexelray-framework's
 * {@code docs/plans/chains.md}, step 5.
 */
public final class Undo {

    /** What an undo or redo request did. */
    public enum Kind {
        /** The latest group was put back. */
        UNDONE,
        /** The latest undone group was put forward again. */
        REDONE,
        /** Something changed what the group left, or its changes were interleaved with another's; nothing moved. */
        CONFLICT,
        /** There was nothing to undo, or nothing to redo. */
        EMPTY
    }

    /** One request's outcome: what happened, and to which group (0 when {@link Kind#EMPTY}). */
    public record Outcome(Kind kind, long group) {
    }

    /**
     * One ledger's part of a group: put {@code restore} back if the ledger still holds {@code left}, the value the
     * group left it with. Compared as values rather than by version, because a restore is itself a commit, and a
     * version check would then refuse the group below for the version the one above it moved.
     */
    record Span(Ledger<?> ledger, Object restore, Object left) {
    }

    /** One cause's changes, on every ledger it reached. Tangled when another cause's changes came between its own. */
    record Group(long id, List<Span> spans, boolean tangled) {
    }

    /** A ledger's committed change, as it tells the history. */
    record Change(Ledger<?> ledger, long group, Object before, Object after) {
    }

    /** What can be undone and redone: the history's state. Read through {@link #past()}. */
    public static final class Past {

        private final List<Group> undo;
        private final List<Group> redo;

        Past(List<Group> undo, List<Group> redo) {
            this.undo = undo;
            this.redo = redo;
        }

        /** Whether there is a group to undo. */
        public boolean canUndo() {
            return !undo.isEmpty();
        }

        /** Whether there is a group to redo. */
        public boolean canRedo() {
            return !redo.isEmpty();
        }

        /** How many groups can be undone. */
        public int undoDepth() {
            return undo.size();
        }

        /** How many groups can be redone. */
        public int redoDepth() {
            return redo.size();
        }

        /** {@code change} added to its group, or to a new one at the top; a new group clears what could be redone. */
        Past recorded(Change change, int depth) {
            List<Group> next = new ArrayList<>(undo);
            for (int i = next.size() - 1; i >= 0; i--) {
                Group g = next.get(i);
                if (g.id() == change.group()) {
                    next.set(i, merged(g, change));
                    return new Past(List.copyOf(next), redo);
                }
            }
            next.add(new Group(change.group(), List.of(new Span(change.ledger(), change.before(),
                    change.after())), false));
            while (next.size() > depth) {
                next.remove(0);
            }
            return new Past(List.copyOf(next), List.of());
        }

        private static Group merged(Group g, Change change) {
            List<Span> spans = new ArrayList<>(g.spans());
            boolean tangled = g.tangled();
            for (int i = 0; i < spans.size(); i++) {
                Span s = spans.get(i);
                if (s.ledger() == change.ledger()) {
                    tangled |= !Objects.equals(s.left(), change.before());
                    spans.set(i, new Span(s.ledger(), s.restore(), change.after()));
                    return new Group(g.id(), List.copyOf(spans), tangled);
                }
            }
            spans.add(new Span(change.ledger(), change.before(), change.after()));
            return new Group(g.id(), List.copyOf(spans), tangled);
        }

        @Override
        public String toString() {
            return "Past[undo " + undo.size() + ", redo " + redo.size() + "]";
        }
    }

    private final Ledger<Past> history;
    private final Step<Past, Change> record;
    private final Step<Past, Boolean> move;
    private final Topic<Outcome> outcomes;
    private final int depth;
    private final Inbox.Between undoNow = from -> walk(from, true);
    private final Inbox.Between redoNow = from -> walk(from, false);

    private Undo(Inbox owner, String topicName, int capacity, int depth) {
        this.depth = depth;
        this.outcomes = Topic.edge(topicName + ".outcome", Outcome.class);
        Ledger.Builder<Past> b = Ledger.of(new Past(List.of(), List.of()));
        this.record = b.step("record", (past, change, out) -> past.recorded(change, this.depth));
        this.move = b.step("move", (past, undoing, out) -> {
            out.between(undoing ? undoNow : redoNow);
            return past;
        });
        this.history = b.build(owner, topicName, capacity);
    }

    /**
     * An undo history, owned by {@code owner}: its requests and records arrive on a topic named {@code topicName},
     * holding at most {@code capacity}, and it keeps the latest {@code depth} groups.
     */
    public static Undo build(Inbox owner, String topicName, int capacity, int depth) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(topicName, "topicName");
        if (depth < 1) {
            throw new IllegalArgumentException("an undo history keeps at least one group: " + depth);
        }
        return new Undo(owner, topicName, capacity, depth);
    }

    /** Where {@link #undo()} and {@link #redo()} are sent. */
    public Topic<Step.Transaction<Past>> topic() {
        return history.topic();
    }

    /** The request to undo the latest group. */
    public Step.Transaction<Past> undo() {
        return move.with(Boolean.TRUE);
    }

    /** The request to redo the latest undone group. */
    public Step.Transaction<Past> redo() {
        return move.with(Boolean.FALSE);
    }

    /** Where each request's {@link Outcome} is said. */
    public Topic<Outcome> outcomes() {
        return outcomes;
    }

    /** What can be undone and redone, for reading from any thread. */
    public State<Past> past() {
        return history.state();
    }

    /** A ledger committed {@code before} → {@code after} for {@code group}: tell the history, in the round's sends. */
    void record(Ledger<?> ledger, long group, Versioned<?> before, Versioned<?> after, Inbox from) {
        from.post(history.topic(), record.with(new Change(ledger, group, before.value(), after.value())));
    }

    /** Undo or redo the latest group, on the coordinator between rounds: check every ledger, then restore all or none. */
    private void walk(Inbox from, boolean undoing) {
        Past past = history.state().value();
        List<Group> source = undoing ? past.undo : past.redo;
        if (source.isEmpty()) {
            from.post(outcomes, new Outcome(Kind.EMPTY, 0));
            return;
        }
        Group g = source.get(source.size() - 1);
        boolean current = !g.tangled();
        for (Span s : g.spans()) {
            current &= Objects.equals(s.ledger().state().value(), s.left());
        }
        if (!current) {
            from.post(outcomes, new Outcome(Kind.CONFLICT, g.id()));
            return;
        }
        List<Span> inverse = new ArrayList<>(g.spans().size());
        for (Span s : g.spans()) {
            s.ledger().restore(s.restore());
            inverse.add(new Span(s.ledger(), s.left(), s.restore()));
        }
        Group moved = new Group(g.id(), List.copyOf(inverse), false);
        List<Group> remaining = new ArrayList<>(source);
        remaining.remove(remaining.size() - 1);
        List<Group> to = new ArrayList<>(undoing ? past.redo : past.undo);
        to.add(moved);
        history.restore(undoing ? new Past(List.copyOf(remaining), List.copyOf(to))
                : new Past(List.copyOf(to), List.copyOf(remaining)));
        from.post(outcomes, new Outcome(undoing ? Kind.UNDONE : Kind.REDONE, g.id()));
    }

    @Override
    public String toString() {
        return "Undo[" + history.topic().name() + "]";
    }
}
