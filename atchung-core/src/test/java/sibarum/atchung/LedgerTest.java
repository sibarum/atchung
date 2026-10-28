package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link Ledger}: a state its owner changes by running declared steps sent to it, one at a time, all or nothing.
 *
 * <p>Each test is one sentence of {@code Ledger}'s javadoc made to fail if it stops being true.
 */
class LedgerTest {

    private static final Topic<String> SAID = Topic.of("test.said", String.class);

    record Notes(List<String> lines) {
        Notes with(String line) {
            List<String> next = new ArrayList<>(lines);
            next.add(line);
            return new Notes(List.copyOf(next));
        }
    }

    private static long later() {
        return System.nanoTime() + 60_000_000_000L;
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

    @Test
    void aTransactionRunsOnTheOwnerAtTheNextTickAndCommitsASnapshot() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> add = b.step("add", (notes, line, out) -> notes.with(line));
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);

        bus.publish(notes.topic(), add.with("one"));

        assertEquals(0, notes.state().version(), "nothing runs on the sender's thread");
        tick.run(later());
        assertEquals(List.of("one"), notes.state().value().lines());
        assertEquals(1, notes.state().version());
    }

    @Test
    void transactionsRunOneAtATimeEachOnWhatThePreviousLeft() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> add = b.step("add", (notes, line, out) -> notes.with(line + notes.lines().size()));
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);

        bus.publish(notes.topic(), add.with("a"));
        bus.publish(notes.topic(), add.with("b"));
        bus.publish(notes.topic(), add.with("c"));
        tick.run(later());

        assertEquals(List.of("a0", "b1", "c2"), notes.state().value().lines());
        assertEquals(3, notes.state().version());
    }

    @Test
    void whatAStepSendsReachesItsSubscribersWhenTheRoundEnds() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> heard = new ArrayList<>();
        tick.inbox("listener").subscribe(SAID, heard::add, 16);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> add = b.step("add", (notes, line, out) -> {
            out.send(SAID, "added " + line);
            return notes.with(line);
        });
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);

        bus.publish(notes.topic(), add.with("one"));
        tick.run(later());

        assertEquals(List.of("added one"), heard);
        assertEquals(2, tick.lastRounds(), "the step's round, then the round its send was delivered in");
    }

    @Test
    void aStepThatThrowsCommitsNothingAndItsSendsAreTakenBack() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> heard = new ArrayList<>();
        tick.inbox("listener").subscribe(SAID, heard::add, 16);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> add = b.step("add", (notes, line, out) -> {
            out.send(SAID, "added " + line);
            return notes.with(line);
        });
        Step<Notes, String> fail = b.step("fail", (notes, line, out) -> {
            out.send(SAID, "about to fail");
            throw new IllegalStateException("no");
        });
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);

        bus.publish(notes.topic(), add.with("kept"));
        bus.publish(notes.topic(), fail.with("lost"));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> tick.run(later()));
        assertEquals("no", thrown.getMessage(), "the throw goes where any subscriber's goes");
        assertEquals(List.of("kept"), notes.state().value().lines());
        assertEquals(1, notes.state().version());
        tick.run(later());
        assertEquals(List.of("added kept"), heard, "the step before it sent; the one that threw sent nothing");
    }

    @Test
    void aStepThatChangesNothingCommitsNoVersionButStillSends() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> heard = new ArrayList<>();
        tick.inbox("listener").subscribe(SAID, heard::add, 16);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> ping = b.step("ping", (notes, s, out) -> {
            out.send(SAID, s);
            return notes;
        });
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);

        bus.publish(notes.topic(), ping.with("hello"));
        tick.run(later());

        assertEquals(0, notes.state().version());
        assertEquals(List.of("hello"), heard);
    }

    @Test
    void aStepDeclaredOnAnotherLedgerIsRefused() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<Notes> mine = Ledger.of(new Notes(List.of()));
        mine.<String>step("add", (notes, line, out) -> notes.with(line));
        Ledger<Notes> notes = mine.build(tick.inbox("notes"), "test.notes", 16);
        Step<Notes, String> foreign = Ledger.of(new Notes(List.of()))
                .step("add", (n, line, out) -> n.with(line));

        bus.publish(notes.topic(), foreign.with("x"));

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> tick.run(later()));
        assertEquals("step not declared on the ledger at 'test.notes': add", thrown.getMessage());
        assertEquals(0, notes.state().version());
    }

    @Test
    void anOutKeptPastItsStepRefuses() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Out[] kept = new Out[1];
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> keep = b.step("keep", (notes, s, out) -> {
            kept[0] = out;
            return notes;
        });
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);
        bus.publish(notes.topic(), keep.with("x"));
        tick.run(later());

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> kept[0].send(SAID, "late"));
        assertEquals("an Out sends only from inside one of its ledger's steps, on the thread running it; this one "
                + "belongs to inbox 'notes'", thrown.getMessage());
    }

    @Test
    void aTransactionSentFromAnotherComponentsDeliveryRunsTheRoundAfter() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<Notes> b = Ledger.of(new Notes(List.of()));
        Step<Notes, String> add = b.step("add", (notes, line, out) -> notes.with(line));
        Ledger<Notes> notes = b.build(tick.inbox("notes"), "test.notes", 16);
        Inbox view = tick.inbox("view");
        Sender<Step.Transaction<Notes>> toNotes = view.sender(notes.topic());
        view.subscribe(SAID, s -> toNotes.send(add.with(s)), 16);

        bus.publish(SAID, "clicked");
        tick.run(later());

        assertEquals(List.of("clicked"), notes.state().value().lines());
        assertEquals(2, tick.lastRounds());
    }
}
