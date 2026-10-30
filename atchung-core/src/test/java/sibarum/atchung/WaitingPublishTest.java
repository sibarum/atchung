package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Atchung#publishWaiting}: an edge for a full tick mailbox waits for a tick to make room, where
 * {@link Atchung#publish} would overflow it.
 *
 * <p>{@link Fatal#THROW} is installed so that an overflow, which is what this exists to prevent, fails the test rather
 * than halting the JVM.
 */
class WaitingPublishTest {

    private static final Topic<String> EDGES = Topic.of("test.waiting.edges", String.class);

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
    void aFullMailboxMakesThePublisherWaitRatherThanOverflow() throws Exception {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new CopyOnWriteArrayList<>();
        tick.inbox("c").subscribe(EDGES, seen::add, 2);

        Thread publisher = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                bus.publishWaiting(EDGES, "m" + i);
            }
        }, "publisher");
        publisher.start();

        // Ticks on this thread until every message has arrived; each makes room for the next two.
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen.size() < 10 && System.nanoTime() < until) {
            tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);
            Thread.onSpinWait();
        }
        publisher.join(1000);

        assertEquals(List.of("m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"), seen,
                "every message arrives, in order, and none overflowed a mailbox of two");
        assertFalse(publisher.isAlive());
    }

    @Test
    void aWaitingPublisherThatIsInterruptedGivesUpAndKeepsTheInterrupt() throws Exception {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        tick.inbox("c").subscribe(EDGES, s -> { }, 1);
        bus.publishWaiting(EDGES, "fills it");

        boolean[] result = new boolean[2];
        Thread publisher = new Thread(() -> {
            result[0] = bus.publishWaiting(EDGES, "waits");
            result[1] = Thread.currentThread().isInterrupted();
        }, "publisher");
        publisher.start();
        while (publisher.getState() != Thread.State.WAITING) {
            Thread.onSpinWait();
        }
        publisher.interrupt();
        publisher.join(1000);

        assertFalse(result[0], "an interrupted wait says the message was not delivered");
        assertTrue(result[1], "and leaves the interrupt for the publisher's next blocking read");
        assertEquals(1, tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD), "only the first was staged");
    }

    @Test
    void aWaitingSenderBeginsACauseOfItsSourceForEachSend() {
        sibarum.probe.Cause.Source source = sibarum.probe.Cause.source("test.waiting.source", sibarum.probe.Cause.Kind.IO);
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Long> causes = new CopyOnWriteArrayList<>();
        tick.inbox("c").subscribe(EDGES, s -> causes.add(sibarum.probe.Cause.current()), 4);
        Sender<String> sender = Sender.waiting(bus, EDGES, source);

        sender.send("a");
        sender.send("b");
        tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);

        assertEquals(2, causes.size());
        for (long c : causes) {
            assertEquals(source.index(), sibarum.probe.Cause.sourceOf(c), "each message carries the source's cause");
        }
        assertTrue(causes.get(0) != causes.get(1), "and a cause of its own");
    }

    @Test
    void aStateTopicNeverWaits() {
        Topic<String> state = Topic.state("test.waiting.state", String.class, s -> "one");
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new CopyOnWriteArrayList<>();
        tick.inbox("c").subscribe(state, seen::add, 1);

        for (int i = 0; i < 5; i++) {
            assertTrue(bus.publishWaiting(state, "v" + i));
        }
        tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);

        assertEquals(List.of("v4"), seen, "a state topic folds, so it cannot fill and never waits");
    }
}
