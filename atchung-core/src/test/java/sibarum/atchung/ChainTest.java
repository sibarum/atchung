package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Chain}: steps on several ledgers, each one's outcome the next one's arguments, picked by type.
 *
 * <p>The chain is the plan's witness, closing a document: a click on the view asks the editor to close, a clean
 * document closes, and a dirty one asks the dialogs ledger to prompt, which holds the run until the person answers.
 * Each test is one sentence of {@code Chain}'s javadoc made to fail if it stops being true.
 */
class ChainTest {

    record Click(int doc) {
    }

    record CloseAsked() {
    }

    record PromptToSave() {
    }

    record Closed() {
    }

    interface Answer {
    }

    record Save() implements Answer {
    }

    record DontSave() implements Answer {
    }

    record Cancel() implements Answer {
    }

    record View(int clicks, int closings) {
        View(int clicks) {
            this(clicks, 0);
        }
    }

    record Doc(boolean dirty, boolean open, boolean saved) {
        Doc closed(boolean save) {
            return new Doc(false, false, save);
        }
    }

    record Prompts(Chain.Resume waiting) {
    }

    private static final Topic<String> SHOWN = Topic.of("test.shown", String.class);
    private static final Topic<Answer> ANSWERS = Topic.of("test.answers", Answer.class);

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

    /** The witness: three ledgers on three inboxes, and the chain across them. */
    private static final class Close {
        final Atchung bus = Atchung.create();
        final Tick tick = bus.tick(null);
        final List<String> shown = new ArrayList<>();
        final List<Long> runs = new ArrayList<>();

        final Ledger<View> view;
        final Ledger<Doc> editor;
        final Ledger<Prompts> dialogs;
        final Step<View, Click> closeClicked;
        final Step<Prompts, Answer> answered;
        final Chain<View, Click> chain;

        Close(boolean dirty) {
            tick.inbox("listener").subscribe(SHOWN, shown::add, 16);

            Ledger.Builder<View> vb = Ledger.of(new View(0));
            closeClicked = vb.step("closeClicked", (v, click, out) -> {
                runs.add(out.run());
                out.result(new CloseAsked());
                return new View(v.clicks() + 1, v.closings());
            });
            Step<View, Closed> closed = vb.step("closed", (v, c, out) -> new View(v.clicks(), v.closings() + 1));
            view = vb.build(tick.inbox("view"), "test.view", 16);

            Ledger.Builder<Doc> eb = Ledger.of(new Doc(dirty, true, false));
            Step<Doc, CloseAsked> closeRequested = eb.step("closeRequested", (d, asked, out) -> {
                runs.add(out.run());
                if (d.dirty()) {
                    out.result(new PromptToSave());
                    return d;
                }
                out.result(new Closed());
                return d.closed(false);
            });
            Step<Doc, Save> saveAndClose = eb.step("saveAndClose", (d, save, out) -> {
                runs.add(out.run());
                out.result(new Closed());
                return d.closed(true);
            });
            Step<Doc, DontSave> discardAndClose = eb.step("discardAndClose", (d, dont, out) -> {
                runs.add(out.run());
                out.result(new Closed());
                return d.closed(false);
            });
            editor = eb.build(tick.inbox("editor"), "test.editor", 16);

            Ledger.Builder<Prompts> db = Ledger.of(new Prompts(null));
            Step<Prompts, PromptToSave> askSave = db.step("askSave", (p, ask, out) -> {
                runs.add(out.run());
                out.send(SHOWN, "save?");
                return new Prompts(out.hold());
            });
            answered = db.step("answered", (p, answer, out) -> {
                out.resume(p.waiting(), answer);
                return new Prompts(null);
            });
            dialogs = db.on(ANSWERS, answered).build(tick.inbox("dialogs"), "test.dialogs", 16);

            chain = Chain.start("close", view, closeClicked)
                    .then(CloseAsked.class, editor, closeRequested)
                    .then(PromptToSave.class, dialogs, askSave)
                    .then(Save.class, editor, saveAndClose)
                    .or(DontSave.class, editor, discardAndClose)
                    .then(Closed.class, view, closed)
                    .build();
        }

        void click() {
            bus.publish(chain.topic(), chain.with(new Click(1)));
            tick.run(later());
        }
    }

    @Test
    void eachStepRunsOnItsOwnLedgerWithThePreviousOutcome() {
        Close c = new Close(false);

        c.click();

        assertEquals(1, c.view.state().value().clicks());
        assertEquals(new Doc(false, false, false), c.editor.state().value());
        assertEquals(2, c.tick.lastRounds(), "the click's round, then the editor's");
    }

    @Test
    void anOutcomeOfAnotherTypeEndsTheRun() {
        Close c = new Close(false);

        c.click();

        assertNull(c.dialogs.state().value().waiting(), "Closed is not a PromptToSave, so nothing prompted");
        assertEquals(List.of(), c.shown);
        assertEquals(0, c.dialogs.state().version());
    }

    @Test
    void aStepThatWaitsHoldsTheRunAndALaterStepResumesIt() {
        Close c = new Close(true);

        c.click();

        assertNotNull(c.dialogs.state().value().waiting(), "the prompt holds the run");
        assertEquals(List.of("save?"), c.shown);
        assertTrue(c.editor.state().value().open(), "nothing closes while the person decides");

        c.bus.publish(c.dialogs.topic(), c.answered.with(new Save()));
        c.tick.run(later());

        assertEquals(new Doc(false, false, true), c.editor.state().value());
        assertNull(c.dialogs.state().value().waiting());
    }

    @Test
    void anAnswerSentAsAPlainMessageResumesTheRunThroughAHeardTopic() {
        Close c = new Close(true);

        c.click();
        c.bus.publish(ANSWERS, new DontSave());
        c.tick.run(later());

        assertEquals(new Doc(false, false, false), c.editor.state().value());
        assertEquals(1, c.view.state().value().closings(), "the branch joined the link after it");
    }

    @Test
    void eachAnswerTakesItsOwnBranchAndTheBranchesJoin() {
        Close saved = new Close(true);
        saved.click();
        saved.bus.publish(saved.dialogs.topic(), saved.answered.with(new Save()));
        saved.tick.run(later());

        Close discarded = new Close(true);
        discarded.click();
        discarded.bus.publish(discarded.dialogs.topic(), discarded.answered.with(new DontSave()));
        discarded.tick.run(later());

        assertTrue(saved.editor.state().value().saved());
        assertEquals(new Doc(false, false, false), discarded.editor.state().value());
        assertEquals(1, saved.view.state().value().closings(), "Save's branch went on to the link after it");
        assertEquals(1, discarded.view.state().value().closings(), "and so did Don't save's");
    }

    @Test
    void anAnswerNoAlternativeTakesEndsTheRun() {
        Close c = new Close(true);

        c.click();
        c.bus.publish(c.dialogs.topic(), c.answered.with(new Cancel()));
        c.tick.run(later());

        assertEquals(new Doc(true, true, false), c.editor.state().value(), "Cancel closes nothing");
        assertEquals(0, c.view.state().value().closings());
        assertNull(c.dialogs.state().value().waiting(), "the prompt is answered all the same");
    }

    @Test
    void theFirstAlternativeThatTakesTheOutcomeRunsAsACatchDoes() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<View> sb = Ledger.of(new View(0));
        Step<View, Answer> start = sb.step("start", (v, a, out) -> {
            out.result(a);
            return v;
        });
        Ledger<View> source = sb.build(tick.inbox("source"), "test.source", 16);
        Ledger.Builder<Doc> tb = Ledger.of(new Doc(true, true, false));
        Step<Doc, Save> save = tb.step("save", (d, s, out) -> d.closed(true));
        Step<Doc, Answer> any = tb.step("any", (d, a, out) -> d.closed(false));
        Ledger<Doc> target = tb.build(tick.inbox("target"), "test.target", 16);
        Chain<View, Answer> chain = Chain.start("catch", source, start)
                .then(Save.class, target, save)
                .or(Answer.class, target, any)
                .build();

        bus.publish(chain.topic(), chain.with(new Save()));
        tick.run(later());
        assertTrue(target.state().value().saved(), "Save is declared first, so Save takes it");

        bus.publish(chain.topic(), chain.with(new Cancel()));
        tick.run(later());
        assertEquals(new Doc(false, false, false), target.state().value(), "anything else falls to Answer");
    }

    @Test
    void anAlternativeAnEarlierOneAlreadyTakesIsRefused() {
        Tick tick = Atchung.create().tick(null);
        Ledger.Builder<View> sb = Ledger.of(new View(0));
        Step<View, Answer> start = sb.step("start", (v, a, out) -> v);
        Ledger<View> source = sb.build(tick.inbox("source"), "test.source", 16);
        Ledger.Builder<Doc> tb = Ledger.of(new Doc(true, true, false));
        Step<Doc, Answer> any = tb.step("any", (d, a, out) -> d);
        Step<Doc, Save> save = tb.step("save", (d, s, out) -> d);
        Ledger<Doc> target = tb.build(tick.inbox("target"), "test.target", 16);
        Chain.Builder<View, Answer> b = Chain.start("unreachable", source, start).then(Answer.class, target, any);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> b.or(Save.class, target, save));
        assertEquals("chain 'unreachable': step 'save' takes Save, which step 'any' before it in the same link already "
                + "takes as Answer, so it could never run", thrown.getMessage());
    }

    @Test
    void orNeedsALinkToAddTo() {
        Tick tick = Atchung.create().tick(null);
        Ledger.Builder<View> sb = Ledger.of(new View(0));
        Step<View, Answer> start = sb.step("start", (v, a, out) -> v);
        Step<View, Save> other = sb.step("other", (v, s, out) -> v);
        Ledger<View> source = sb.build(tick.inbox("source"), "test.source", 16);
        Chain.Builder<View, Answer> b = Chain.start("first", source, start);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> b.or(Save.class, source, other));
        assertEquals("chain 'first': or(...) adds to a link made by then(...); a run's first step is the one it starts "
                + "with", thrown.getMessage());
    }

    @Test
    void everyStepOfARunHasItsIdAndEachRunItsOwn() {
        Close c = new Close(true);

        c.click();
        c.bus.publish(c.dialogs.topic(), c.answered.with(new DontSave()));
        c.tick.run(later());
        c.click();

        assertEquals(6, c.runs.size(), "view, editor, dialogs, editor; then view, editor, the document now clean");
        long first = c.runs.get(0);
        assertEquals(List.of(first, first, first, first), c.runs.subList(0, 4), "a held run keeps its id");
        long second = c.runs.get(4);
        assertNotEquals(first, second);
        assertEquals(List.of(second, second), c.runs.subList(4, 6));
    }

    @Test
    void aStepThatThrowsTakesItsOutcomeBackWithEverythingElse() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<View> ab = Ledger.of(new View(0));
        Step<View, Click> fails = ab.step("fails", (v, click, out) -> {
            out.result(new CloseAsked());
            throw new IllegalStateException("no");
        });
        Ledger<View> a = ab.build(tick.inbox("a"), "test.a", 16);
        Ledger.Builder<View> bb = Ledger.of(new View(0));
        Step<View, CloseAsked> count = bb.step("count", (v, asked, out) -> new View(v.clicks() + 1));
        Ledger<View> b = bb.build(tick.inbox("b"), "test.b", 16);
        Chain<View, Click> chain = Chain.start("throws", a, fails).then(CloseAsked.class, b, count).build();

        bus.publish(chain.topic(), chain.with(new Click(1)));

        assertThrows(IllegalStateException.class, () -> tick.run(later()));
        tick.run(later());
        assertEquals(0, b.state().version(), "the run did not go on");
    }

    @Test
    void anOutcomeOutsideAChainGoesNowhere() {
        Close c = new Close(false);

        c.bus.publish(c.view.topic(), c.closeClicked.with(new Click(1)));
        c.tick.run(later());

        assertEquals(1, c.view.state().value().clicks());
        assertEquals(0, c.editor.state().version());
        assertEquals(List.of(0L), c.runs, "a step sent on its own is in no run");
    }

    @Test
    void aStepGoesOnOnce() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<View> ab = Ledger.of(new View(0));
        Step<View, Click> twice = ab.step("twice", (v, click, out) -> {
            out.result(new CloseAsked());
            out.result(new Closed());
            return v;
        });
        Ledger<View> a = ab.build(tick.inbox("a"), "test.a", 16);
        Chain<View, Click> chain = Chain.start("twice", a, twice).build();

        bus.publish(chain.topic(), chain.with(new Click(1)));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> tick.run(later()));
        assertEquals("a step gives one outcome or holds its run, once", thrown.getMessage());
    }

    @Test
    void aStepNotDeclaredOnItsLedgerIsRefusedWhenTheChainIsBuilt() {
        Tick tick = Atchung.create().tick(null);
        Ledger.Builder<View> ab = Ledger.of(new View(0));
        Step<View, Click> mine = ab.step("mine", (v, click, out) -> v);
        Ledger<View> a = ab.build(tick.inbox("a"), "test.a", 16);
        Ledger<View> b = Ledger.of(new View(0)).build(tick.inbox("b"), "test.b", 16);
        Step<View, CloseAsked> elsewhere = Ledger.of(new View(0)).step("elsewhere", (v, asked, out) -> v);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> Chain.start("bad", a, mine).then(CloseAsked.class, b, elsewhere));
        assertEquals("chain 'bad': step 'elsewhere' is not declared on the ledger at 'test.b'", thrown.getMessage());
    }
}
