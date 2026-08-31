package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * A folding mailbox: the queue grows with how much changed, not with how often it was said.
 *
 * <p>The load-bearing assertions are the two that make folding safe rather than merely cheap — a superseding
 * write never needs a slot, so it cannot overflow a mailbox however fast it repeats; and the survivor takes the
 * <em>latest</em> write's position in the queue, so it stays ordered against the edges it must stay ordered
 * against.
 */
class FoldingMailboxTest {

    /** A write of {@code value} to {@code cell}, or — with a null cell — an edge that folds with nothing. */
    private record Write(String cell, String value) {
    }

    private static final Topic<Write> WRITES = Topic.of("test.writes", Write.class);

    /** Writes fold by cell; an event with no cell is an edge. */
    private static final Fold<Write> BY_CELL = Write::cell;

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
    void repeatedWritesToOneCellOccupyOneSlot() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 2, Backpressure.FAIL, BY_CELL);

        for (int i = 0; i < 100_000; i++) {
            bus.publish(WRITES, new Write("text", "v" + i));
        }
        pump.drain();

        assertEquals(List.of(new Write("text", "v99999")), seen,
                "a hundred thousand writes to one cell are one write: only the last was ever observable");
    }

    @Test
    void aSupersedingWriteNeverOverflowsHoweverFullTheMailboxIs() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(WRITES, e -> { }, 2, Backpressure.FAIL, BY_CELL);

        bus.publish(WRITES, new Write("a", "1"));
        bus.publish(WRITES, new Write("b", "1"));   // mailbox is now full at 2 cells

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 1000; i++) {
                bus.publish(WRITES, new Write("a", "x" + i));
                bus.publish(WRITES, new Write("b", "y" + i));
            }
        }, "writing a cell that is already queued takes no new slot, so a full mailbox is no obstacle to it");
    }

    @Test
    void aNewCellStillOverflowsAFullMailbox() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(WRITES, e -> { }, 2, Backpressure.FAIL, BY_CELL);

        bus.publish(WRITES, new Write("a", "1"));
        bus.publish(WRITES, new Write("b", "1"));

        org.junit.jupiter.api.Assertions.assertThrows(MailboxOverflow.class,
                () -> bus.publish(WRITES, new Write("c", "1")),
                "folding bounds the queue by cells; a third cell is a third cell");
    }

    @Test
    void theSurvivingWriteTakesTheLatestWritesPosition() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 8, Backpressure.FAIL, BY_CELL);

        bus.publish(WRITES, new Write("a", "1"));
        bus.publish(WRITES, new Write(null, "edge"));   // e.g. "destroy the thing cell 'a' belongs to"
        bus.publish(WRITES, new Write("a", "2"));
        pump.drain();

        assertEquals(List.of(new Write(null, "edge"), new Write("a", "2")), seen,
                "the write that won arrives after the edge, exactly where the unfolded sequence had it — "
                        + "folding must not carry a write back in front of something that invalidates it");
    }

    @Test
    void edgesNeverFoldWithEachOther() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 8, Backpressure.FAIL, BY_CELL);

        bus.publish(WRITES, new Write(null, "one"));
        bus.publish(WRITES, new Write(null, "one"));   // equal values, and still two events
        pump.drain();

        assertEquals(2, seen.size(),
                "an event that names no cell is an occurrence, and two occurrences are not one");
    }

    @Test
    void interleavedCellsKeepTheirOwnLatestValues() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 8, Backpressure.FAIL, BY_CELL);

        bus.publish(WRITES, new Write("a", "1"));
        bus.publish(WRITES, new Write("b", "1"));
        bus.publish(WRITES, new Write("a", "2"));
        bus.publish(WRITES, new Write("b", "2"));
        bus.publish(WRITES, new Write("a", "3"));
        pump.drain();

        assertEquals(List.of(new Write("b", "2"), new Write("a", "3")), seen,
                "each cell keeps its latest write, and both sit where their latest write arrived");
    }

    @Test
    void aMailboxWithNoFoldIsUnchanged() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 8, Backpressure.FAIL);

        bus.publish(WRITES, new Write("a", "1"));
        bus.publish(WRITES, new Write("a", "2"));
        pump.drain();

        assertEquals(2, seen.size(), "without a Fold, two writes to one cell are two events, as they always were");
    }
}
