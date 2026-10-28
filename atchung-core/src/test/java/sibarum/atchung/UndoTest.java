package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Undo}: undoable ledgers' changes, grouped by cause, put back a group at a time, all or nothing.
 *
 * <p>Each test is one sentence of {@code Undo}'s javadoc made to fail if it stops being true.
 */
class UndoTest {

    record Click() {
    }

    record Asked() {
    }

    record Answer() {
    }

    private Fatal previous;

    @BeforeEach
    void substituteTheFatalPolicy() {
        previous = Atchung.fatal();
        Atchung.onFatal(Fatal.THROW);
    }

    @AfterEach
    void restoreIt() {
        Atchung.onFatal(previous);
    }

    private static long later() {
        return System.nanoTime() + 60_000_000_000L;
    }

    /** A history, two undoable ledgers of strings, and their steps. */
    private static final class Setup {
        final Atchung bus = Atchung.create();
        final Tick tick = bus.tick(null);
        final Undo undo = Undo.build(tick.inbox("history"), "test.history", 64, 10);
        final List<Undo.Outcome> outcomes = new ArrayList<>();

        final Ledger<String> a;
        final Ledger<String> b;
        final Step<String, String> setA;
        final Step<String, String> setB;
        final Step<String, Click> clicked;
        final Step<String, Asked> asked;

        Setup() {
            tick.inbox("listener").subscribe(undo.outcomes(), outcomes::add, 64);
            Ledger.Builder<String> ab = Ledger.of("a0").undoable(undo);
            setA = ab.step("setA", (s, v, out) -> v);
            clicked = ab.step("clicked", (s, c, out) -> {
                out.result(new Asked());
                return s + "+";
            });
            a = ab.build(tick.inbox("a"), "test.a", 16);
            Ledger.Builder<String> bb = Ledger.of("b0").undoable(undo);
            setB = bb.step("setB", (s, v, out) -> v);
            asked = bb.step("asked", (s, q, out) -> s + "!");
            b = bb.build(tick.inbox("b"), "test.b", 16);
        }

        void send(Ledger<String> to, Step.Transaction<String> t) {
            bus.publish(to.topic(), t);
            tick.run(later());
        }

        void undo() {
            bus.publish(undo.topic(), undo.undo());
            tick.run(later());
        }

        void redo() {
            bus.publish(undo.topic(), undo.redo());
            tick.run(later());
        }

        List<Undo.Kind> kinds() {
            List<Undo.Kind> k = new ArrayList<>();
            for (Undo.Outcome o : outcomes) {
                k.add(o.kind());
            }
            return k;
        }
    }

    @Test
    void eachChangeOnItsOwnIsAGroupPutBackLatestFirst() {
        Setup s = new Setup();
        s.send(s.a, s.setA.with("a1"));
        s.send(s.a, s.setA.with("a2"));

        s.undo();
        assertEquals("a1", s.a.state().value());
        s.undo();
        assertEquals("a0", s.a.state().value());
        s.redo();
        assertEquals("a1", s.a.state().value());
        s.redo();
        assertEquals("a2", s.a.state().value());

        assertEquals(List.of(Undo.Kind.UNDONE, Undo.Kind.UNDONE, Undo.Kind.REDONE, Undo.Kind.REDONE), s.kinds());
    }

    @Test
    void aChainRunIsOneGroupOnEveryLedgerItReached() {
        Setup s = new Setup();
        Chain<String, Click> chain = Chain.start("click", s.a, s.clicked).then(Asked.class, s.b, s.asked).build();
        s.bus.publish(chain.topic(), chain.with(new Click()));
        s.tick.run(later());
        assertEquals("a0+", s.a.state().value());
        assertEquals("b0!", s.b.state().value());
        assertEquals(1, s.undo.past().value().undoDepth());

        s.undo();

        assertEquals("a0", s.a.state().value());
        assertEquals("b0", s.b.state().value(), "one undo takes the whole run back");
        s.redo();
        assertEquals("a0+", s.a.state().value());
        assertEquals("b0!", s.b.state().value());
    }

    @Test
    void aStepThatResumesAHeldRunIsPartOfIt() {
        Setup s = new Setup();
        Ledger.Builder<Chain.Resume> pb = Ledger.of(new Chain.Resume(null)).undoable(s.undo);
        Step<Chain.Resume, Asked> hold = pb.step("hold", (r, q, out) -> out.hold());
        Step<Chain.Resume, Answer> answered = pb.step("answered", (r, a, out) -> {
            out.resume(r, a);
            return new Chain.Resume(null);
        });
        Ledger<Chain.Resume> prompts = pb.build(s.tick.inbox("prompts"), "test.prompts", 16);
        Ledger.Builder<String> cb = Ledger.of("c0").undoable(s.undo);
        Step<String, Answer> close = cb.step("close", (c, a, out) -> "closed");
        Ledger<String> c = cb.build(s.tick.inbox("c"), "test.c", 16);
        Chain<String, Click> chain = Chain.start("held", s.a, s.clicked)
                .then(Asked.class, prompts, hold)
                .then(Answer.class, c, close)
                .build();

        s.bus.publish(chain.topic(), chain.with(new Click()));
        s.tick.run(later());
        s.bus.publish(prompts.topic(), answered.with(new Answer()));
        s.tick.run(later());
        assertEquals("closed", c.state().value());
        assertEquals(1, s.undo.past().value().undoDepth(), "the answer joined the run it resumed");

        s.undo();

        assertEquals("a0", s.a.state().value());
        assertEquals("c0", c.state().value());
        assertEquals(0, s.undo.past().value().undoDepth());
    }

    @Test
    void aLedgerChangedSinceTheGroupIsAConflictAndNothingMoves() {
        Setup s = new Setup();
        Chain<String, Click> chain = Chain.start("click", s.a, s.clicked).then(Asked.class, s.b, s.asked).build();
        s.bus.publish(chain.topic(), chain.with(new Click()));
        s.tick.run(later());

        // A change to b and an undo in the same round: the undo runs before b's change is recorded, so b is not at
        // the version the group left.
        s.bus.publish(s.b.topic(), s.setB.with("b-later"));
        s.bus.publish(s.undo.topic(), s.undo.undo());
        s.tick.run(later());

        assertEquals(List.of(Undo.Kind.CONFLICT), s.kinds());
        assertEquals("a0+", s.a.state().value(), "a is not put back without b");
        assertEquals("b-later", s.b.state().value(), "and b's later change is not lost");
    }

    @Test
    void nothingToUndoOrRedoSaysSo() {
        Setup s = new Setup();

        s.undo();
        s.redo();

        assertEquals(List.of(Undo.Kind.EMPTY, Undo.Kind.EMPTY), s.kinds());
    }

    @Test
    void aNewChangeAfterAnUndoClearsWhatCouldBeRedone() {
        Setup s = new Setup();
        s.send(s.a, s.setA.with("a1"));
        s.undo();
        assertTrue(s.undo.past().value().canRedo());

        s.send(s.a, s.setA.with("a2"));

        assertFalse(s.undo.past().value().canRedo());
        s.redo();
        assertEquals(Undo.Kind.EMPTY, s.outcomes.get(s.outcomes.size() - 1).kind());
        assertEquals("a2", s.a.state().value());
    }

    @Test
    void aRestoreIsNotItselfRecorded() {
        Setup s = new Setup();
        s.send(s.a, s.setA.with("a1"));

        s.undo();

        assertEquals(0, s.undo.past().value().undoDepth());
        assertEquals(1, s.undo.past().value().redoDepth());
    }

    @Test
    void theHistoryKeepsItsDepth() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Undo undo = Undo.build(tick.inbox("history"), "test.history", 64, 2);
        Ledger.Builder<String> ab = Ledger.of("0").undoable(undo);
        Step<String, String> set = ab.step("set", (s, v, out) -> v);
        Ledger<String> a = ab.build(tick.inbox("a"), "test.a", 16);
        for (String v : List.of("1", "2", "3")) {
            bus.publish(a.topic(), set.with(v));
            tick.run(later());
        }

        assertEquals(2, undo.past().value().undoDepth());
        for (int i = 0; i < 3; i++) {
            bus.publish(undo.topic(), undo.undo());
            tick.run(later());
        }
        assertEquals("1", a.state().value(), "the oldest change fell off the end");
    }
}
