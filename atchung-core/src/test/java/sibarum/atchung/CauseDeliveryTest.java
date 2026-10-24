package sibarum.atchung;

import org.junit.jupiter.api.Test;
import sibarum.probe.Cause;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A cause goes wherever its message goes, and nowhere else: every delivery mode runs the handler inside the cause the
 * event was published in, and puts back whatever cause the delivering thread was in before.
 */
class CauseDeliveryTest {

    private record Write(String cell, String value) {
    }

    private static final Topic<String> TOPIC = Topic.of("test.cause", String.class);
    private static final Topic<Write> WRITES = Topic.of("test.cause.writes", Write.class);
    private static final Cause.Source INPUT = Cause.source("test.input", Cause.Kind.INPUT);
    private static final Cause.Source TIMER = Cause.source("test.timer", Cause.Kind.TIMER);

    @Test
    void anInlineHandlerRunsInThePublishersCause() {
        Atchung bus = Atchung.create();
        AtomicLong seen = new AtomicLong();
        bus.subscribe(TOPIC, e -> seen.set(Cause.current()));

        long cause = INPUT.next();
        Cause.within(cause, () -> bus.publish(TOPIC, "a"));

        assertEquals(cause, seen.get());
    }

    @Test
    void anAsyncHandlerRunsInThePublishersCauseOnTheExecutorsThread() throws Exception {
        Atchung bus = Atchung.create();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            List<Long> seen = new ArrayList<>();
            CountDownLatch done = new CountDownLatch(2);
            bus.subscribeAsync(TOPIC, e -> {
                synchronized (seen) {
                    seen.add(Cause.current());
                }
                done.countDown();
            }, pool);

            long first = INPUT.next();
            long second = TIMER.next();
            Cause.within(first, () -> bus.publish(TOPIC, "a"));
            Cause.within(second, () -> bus.publish(TOPIC, "b"));
            assertTrue(done.await(5, TimeUnit.SECONDS));

            assertEquals(List.of(first, second), seen, "each event in its own cause, on a thread that had neither");
            AtomicLong after = new AtomicLong(-1);
            pool.submit(() -> after.set(Cause.current())).get();
            assertEquals(Cause.NONE, after.get(), "and the pool's thread is left in no cause");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aMailboxDeliversEachEventInItsOwnCauseAndRestoresTheDrainers() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Long> seen = new ArrayList<>();
        pump.subscribe(TOPIC, e -> seen.add(Cause.current()), 16);

        long first = INPUT.next();
        long second = TIMER.next();
        Cause.within(first, () -> bus.publish(TOPIC, "a"));
        bus.publish(TOPIC, "b");                                    // published in no cause
        Cause.within(second, () -> bus.publish(TOPIC, "c"));

        long drainer = TIMER.next();
        Cause.within(drainer, () -> {
            pump.drain();
            assertEquals(drainer, Cause.current(), "the drainer is back in its own cause after the batch");
        });

        assertEquals(List.of(first, Cause.NONE, second), seen);
    }

    @Test
    void aMailboxKeepsCausesInStepWithItsQueueAcrossDropsAndDrains() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> events = new ArrayList<>();
        List<Long> causes = new ArrayList<>();
        pump.subscribe("test", TOPIC, e -> {
            events.add(e);
            causes.add(Cause.current());
        }, 2, Backpressure.DROP_OLDEST);

        long[] c = new long[5];
        for (int round = 0; round < 3; round++) {          // reused buffers: the spares must stay paired too
            events.clear();
            causes.clear();
            for (int i = 0; i < c.length; i++) {
                c[i] = INPUT.next();
                String name = "e" + i;
                Cause.within(c[i], () -> bus.publish(TOPIC, name));
            }
            pump.drain();
            assertEquals(List.of("e3", "e4"), events, "the oldest three were dropped");
            assertEquals(List.of(c[3], c[4]), causes, "and their causes with them, not the survivors'");
        }
    }

    @Test
    void aFoldDeliversTheSurvivingWritesCause() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<Long> seen = new ArrayList<>();
        pump.subscribe(WRITES, w -> seen.add(Cause.current()), 4, Backpressure.FAIL, Write::cell);

        long edge = TIMER.next();
        long older = INPUT.next();
        long newer = INPUT.next();
        Cause.within(older, () -> bus.publish(WRITES, new Write("text", "old")));
        Cause.within(edge, () -> bus.publish(WRITES, new Write(null, "edge")));
        Cause.within(newer, () -> bus.publish(WRITES, new Write("text", "new")));
        pump.drain();

        assertEquals(List.of(edge, newer), seen, "the survivor is delivered in the cause of the write that survived");
    }

    @Test
    void aHandlerThatThrowsLeavesNoCauseBehind() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(TOPIC, e -> {
            throw new IllegalStateException("handler failed");
        }, 4);

        Cause.within(INPUT.next(), () -> bus.publish(TOPIC, "a"));
        assertThrows(IllegalStateException.class, pump::drain);

        assertEquals(Cause.NONE, Cause.current(), "the thread that drained is in no cause, as before");
    }

    @Test
    void aHandlerThatPublishesPassesItsCauseOn() {
        Atchung bus = Atchung.create();
        Pump first = bus.pump();
        Pump second = bus.pump();
        Topic<String> next = Topic.of("test.cause.next", String.class);
        AtomicLong seen = new AtomicLong();
        first.subscribe(TOPIC, e -> bus.publish(next, e), 4);
        second.subscribe(next, e -> seen.set(Cause.current()), 4);

        long cause = TIMER.next();
        Cause.within(cause, () -> bus.publish(TOPIC, "a"));
        first.drain();
        second.drain();

        assertEquals(cause, seen.get(), "two mailboxes downstream, still the timer's cause");
        assertNotEquals(Cause.NONE, cause);
    }
}
