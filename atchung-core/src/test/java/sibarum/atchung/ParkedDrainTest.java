package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Pump#drain(long)} — a consumer with no loop of its own.
 *
 * <p>The case this exists for is a component: one thread, one mailbox, and no other reason to wake. Every
 * other consumer on this bus is a host that already has a loop and calls {@link Pump#drain()} once a frame,
 * which is why waiting was not here to begin with.
 *
 * <p>The assertion that matters most is {@link #aParkedConsumerDoesNotBlockPublishers()}. A mailbox whose
 * reader can stall its writers would be a worse bus than one that cannot wait at all.
 */
final class ParkedDrainTest {

    private static final Topic<String> EDITS = Topic.of("test.edits", String.class);

    /** Long enough to outlast a scheduling hiccup, short enough that a hang fails the suite rather than CI. */
    private static final long GENEROUS_MS = 5_000;

    @Test
    void aParkedDrainReturnsWithWhatArrives() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> seen = new CopyOnWriteArrayList<>();
        pump.subscribe(EDITS, seen::add, 8);

        AtomicInteger delivered = new AtomicInteger(-1);
        CountDownLatch done = new CountDownLatch(1);
        Thread consumer = Thread.ofPlatform().name("test-consumer").start(() -> {
            delivered.set(pump.drain(GENEROUS_MS * 1_000_000L));
            done.countDown();
        });

        // Published after the consumer has had a moment to park, which is the interesting order: the wake has
        // to come from the publish rather than from the consumer happening to look at the right time.
        Thread.sleep(50);
        bus.publish(EDITS, "sphere");

        assertTrue(done.await(GENEROUS_MS, TimeUnit.MILLISECONDS), "the parked drain never returned");
        consumer.join();
        assertEquals(1, delivered.get());
        assertEquals(List.of("sphere"), seen);
    }

    /**
     * The park holds nothing a publisher needs.
     *
     * <p>{@code Object.wait} releases the monitor it waits on, and the parked thread holds no mailbox lock at
     * all — so publishing while a consumer is parked is exactly as fast as publishing when none is. Measured
     * rather than asserted from the design: the publish is timed from a second thread while the consumer sits
     * in a park it will not leave until told.
     */
    @Test
    void aParkedConsumerDoesNotBlockPublishers() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        // A mailbox nothing is published to, so the consumer stays parked for the whole measurement.
        pump.subscribe(Topic.of("test.quiet", String.class), event -> { }, 8);

        CountDownLatch parked = new CountDownLatch(1);
        AtomicLong publishNanos = new AtomicLong();
        Thread consumer = Thread.ofPlatform().name("test-parked").start(() -> {
            parked.countDown();
            pump.drain(GENEROUS_MS * 1_000_000L);
        });
        assertTrue(parked.await(GENEROUS_MS, TimeUnit.MILLISECONDS), "the consumer never started");
        Thread.sleep(50);

        // A topic with no pumped subscriber at all: the path a publisher takes past a parked consumer.
        Topic<String> other = Topic.of("test.other", String.class);
        long t0 = System.nanoTime();
        for (int i = 0; i < 1_000; i++) {
            bus.publish(other, "x");
        }
        publishNanos.set(System.nanoTime() - t0);

        pump.wake();
        consumer.join(GENEROUS_MS);

        long millis = publishNanos.get() / 1_000_000L;
        assertTrue(millis < 1_000,
                "a thousand publishes took " + millis + "ms while a consumer was parked, which means the park "
                        + "is holding something they need");
    }

    /** A park that nothing arrives for ends at its own deadline, having delivered nothing. */
    @Test
    void aParkEndsAtItsDeadline() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDITS, event -> { }, 8);

        long t0 = System.nanoTime();
        int delivered = pump.drain(120L * 1_000_000L);
        long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;

        assertEquals(0, delivered);
        assertTrue(elapsedMillis >= 100, "returned after " + elapsedMillis + "ms, so it did not wait at all");
        assertTrue(elapsedMillis < GENEROUS_MS, "waited " + elapsedMillis + "ms for a 120ms timeout");
    }

    /**
     * {@link Pump#wake()} ends a park early, which is how a component is stopped.
     *
     * <p>Without it the only way to stop a parked component is to wait out its timeout, and a shutdown that
     * takes as long as the longest park somebody chose is a shutdown nobody can reason about.
     */
    @Test
    void wakeEndsAParkThatNothingArrivedFor() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDITS, event -> { }, 8);

        CountDownLatch done = new CountDownLatch(1);
        AtomicLong elapsed = new AtomicLong();
        Thread consumer = Thread.ofPlatform().start(() -> {
            long t0 = System.nanoTime();
            pump.drain(GENEROUS_MS * 1_000_000L);
            elapsed.set(System.nanoTime() - t0);
            done.countDown();
        });
        Thread.sleep(50);
        pump.wake();

        assertTrue(done.await(GENEROUS_MS, TimeUnit.MILLISECONDS), "wake did not end the park");
        consumer.join();
        assertTrue(elapsed.get() < GENEROUS_MS * 1_000_000L / 2,
                "the park ran to its timeout rather than being woken");
    }

    /**
     * A wake left over from one park does not skip the next one.
     *
     * <p>The flag is one-shot. If it were not, a component woken for shutdown that then went round its loop
     * once more would spin through every subsequent park, which reads as a busy component nobody can explain.
     */
    @Test
    void aWakeIsNotRememberedIntoTheNextPark() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDITS, event -> { }, 8);

        CountDownLatch first = new CountDownLatch(1);
        Thread consumer = Thread.ofPlatform().start(() -> {
            pump.drain(GENEROUS_MS * 1_000_000L);
            first.countDown();
        });
        Thread.sleep(50);
        pump.wake();
        assertTrue(first.await(GENEROUS_MS, TimeUnit.MILLISECONDS), "the first park never ended");
        consumer.join();

        long t0 = System.nanoTime();
        pump.drain(120L * 1_000_000L);
        long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;
        assertTrue(elapsedMillis >= 100,
                "the second park returned after " + elapsedMillis + "ms, so it inherited the first one's wake");
    }

    /**
     * Many publishers against one parked consumer, repeatedly.
     *
     * <p>Two things at once. The lock order — a publisher takes its mailbox lock and then the pump's, while a
     * parked consumer takes the pump's and then each mailbox's — is the shape that deadlocks if the publisher
     * signals without letting go first, and a deadlock here is a hang rather than a failure. And no event may
     * be lost to the gap between a consumer checking for work and parking for it.
     */
    @Test
    void nothingIsLostOrDeadlockedBetweenPublishersAndAParkingConsumer() throws Exception {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        AtomicInteger seen = new AtomicInteger();
        pump.subscribe(EDITS, event -> seen.incrementAndGet(), 4096);

        int publishers = 4;
        int each = 500;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new CopyOnWriteArrayList<>();
        for (int p = 0; p < publishers; p++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < each; i++) {
                    bus.publish(EDITS, "e");
                }
            }));
        }

        go.countDown();
        long deadline = System.nanoTime() + GENEROUS_MS * 1_000_000L;
        while (seen.get() < publishers * each && System.nanoTime() < deadline) {
            pump.drain(20L * 1_000_000L);
        }
        for (Thread t : threads) {
            t.join(GENEROUS_MS);
        }
        pump.drain(20L * 1_000_000L);

        assertEquals(publishers * each, seen.get(), "events were lost between a check for work and a park");
    }
}
