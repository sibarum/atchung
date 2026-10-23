package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The drain swaps buffers rather than copying them, and recycles its fold slots. These are the guarantees that a
 * swap could break and a copy could not: a handler that publishes, or drains, while its own batch is being walked;
 * a BLOCK publisher woken by the drain; and folding that still coalesces once the buffers have been swapped and
 * the slots reused many times over.
 */
class DrainBufferTest {

    private record Write(String cell, String value) {
    }

    private static final Topic<Write> WRITES = Topic.of("test.drain.writes", Write.class);
    private static final Topic<Integer> NUMBERS = Topic.of("test.drain.numbers", Integer.class);
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
    void aHandlerThatPublishesQueuesForTheNextDrainNotThisOne() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Integer> seen = new ArrayList<>();
        pump.subscribe(NUMBERS, n -> {
            seen.add(n);
            if (n < 100) {
                bus.publish(NUMBERS, n + 100);
            }
        }, 64, Backpressure.FAIL);

        for (int i = 0; i < 3; i++) {
            bus.publish(NUMBERS, i);
        }
        assertEquals(3, pump.drain(), "the batch is what was queued when the drain began");
        assertEquals(List.of(0, 1, 2), seen);
        assertEquals(3, pump.drain(), "what the handlers published waited in the fresh buffer");
        assertEquals(List.of(0, 1, 2, 100, 101, 102), seen);
        assertEquals(0, pump.drain());
    }

    @Test
    void aHandlerThatDrainsAgainNeverSharesABufferWithTheOuterDrain() {
        for (boolean folding : new boolean[] {false, true}) {
            Atchung bus = Atchung.create();
            Pump pump = bus.pump();
            List<String> seen = new ArrayList<>();
            Subscriber<Write> handler = w -> {
                seen.add(w.value());
                if (w.value().startsWith("outer")) {
                    bus.publish(WRITES, new Write(null, "inner-" + w.value()));
                    pump.drain();   // reentrant: takes the fresh buffer while the outer batch is mid-walk
                }
            };
            if (folding) {
                pump.subscribe(WRITES, handler, 64, Backpressure.FAIL, BY_CELL);
            } else {
                pump.subscribe(WRITES, handler, 64, Backpressure.FAIL);
            }
            bus.publish(WRITES, new Write(null, "outer1"));
            bus.publish(WRITES, new Write(null, "outer2"));
            pump.drain();
            // A second round, so the buffers the reentrant drains made and recycled are used again.
            bus.publish(WRITES, new Write(null, "outer3"));
            pump.drain();

            assertEquals(List.of("outer1", "inner-outer1", "outer2", "inner-outer2", "outer3", "inner-outer3"), seen,
                    "folding=" + folding + ": each inner drain delivered exactly its own event, and the outer batch"
                            + " carried on untouched");
            assertEquals(0, pump.drain());
        }
    }

    @Test
    void aBlockedPublisherIsWokenByTheDrain() throws Exception {
        for (boolean folding : new boolean[] {false, true}) {
            Atchung bus = Atchung.create();
            Pump pump = bus.pump();
            List<String> seen = new ArrayList<>();
            if (folding) {
                pump.subscribe(WRITES, w -> seen.add(w.value()), 1, Backpressure.BLOCK, BY_CELL);
            } else {
                pump.subscribe(WRITES, w -> seen.add(w.value()), 1, Backpressure.BLOCK);
            }
            bus.publish(WRITES, new Write("a", "first"));   // the mailbox is now full

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch published = new CountDownLatch(1);
            Thread producer = new Thread(() -> {
                started.countDown();
                bus.publish(WRITES, new Write("b", "second"));   // a new cell: blocks for room
                published.countDown();
            });
            producer.setDaemon(true);
            producer.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertFalse(published.await(200, TimeUnit.MILLISECONDS),
                    "folding=" + folding + ": a full BLOCK mailbox holds its publisher");

            assertEquals(1, pump.drain());
            assertTrue(published.await(5, TimeUnit.SECONDS),
                    "folding=" + folding + ": the drain made room and woke the publisher");
            producer.join(5000);
            assertEquals(1, pump.drain());
            assertEquals(List.of("first", "second"), seen);
        }
    }

    @Test
    void manyBlockedPublishersAllGetThrough() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        AtomicInteger seen = new AtomicInteger();
        pump.subscribe(NUMBERS, n -> seen.incrementAndGet(), 2, Backpressure.BLOCK);
        int producers = 4;
        int each = 500;
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            Thread t = new Thread(() -> {
                for (int i = 0; i < each; i++) {
                    bus.publish(NUMBERS, i);
                }
            });
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (seen.get() < producers * each && System.nanoTime() < deadline) {
            pump.drain(TimeUnit.MILLISECONDS.toNanos(10));
        }
        for (Thread t : threads) {
            t.join(5000);
        }
        assertEquals(producers * each, seen.get(), "every blocked publish was woken and delivered, none lost");
    }

    @Test
    void foldingStillCoalescesAcrossManySwapsAndRecycledSlots() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Write> seen = new ArrayList<>();
        pump.subscribe(WRITES, seen::add, 4, Backpressure.FAIL, BY_CELL);

        for (int round = 0; round < 50; round++) {
            seen.clear();
            bus.publish(WRITES, new Write("x", "x0-" + round));
            bus.publish(WRITES, new Write(null, "edge1-" + round));
            bus.publish(WRITES, new Write("y", "y0-" + round));
            for (int i = 1; i <= 1000; i++) {
                bus.publish(WRITES, new Write("x", "x" + i + "-" + round));
            }
            bus.publish(WRITES, new Write(null, "edge2-" + round));
            bus.publish(WRITES, new Write("y", "y1-" + round));

            assertEquals(4, pump.drain());
            assertEquals(List.of(
                    new Write(null, "edge1-" + round),
                    new Write("x", "x1000-" + round),
                    new Write(null, "edge2-" + round),
                    new Write("y", "y1-" + round)), seen,
                    "round " + round + ": one slot per cell holding its latest write, at that write's position;"
                            + " each edge its own slot");
        }
    }

    @Test
    void foldingEvictsAndCoalescesUnderDropOldest() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> seen = new ArrayList<>();
        pump.subscribe(WRITES, w -> seen.add(w.value()), 2, Backpressure.DROP_OLDEST, BY_CELL);

        bus.publish(WRITES, new Write("a", "a1"));
        bus.publish(WRITES, new Write(null, "e1"));
        bus.publish(WRITES, new Write("a", "a2"));   // supersedes, moves behind e1, takes no slot
        bus.publish(WRITES, new Write(null, "e2"));  // full: evicts the oldest slot, e1
        bus.publish(WRITES, new Write("a", "a3"));   // a's slot must still be indexed after the eviction
        pump.drain();

        assertEquals(List.of("e2", "a3"), seen);
    }
}
