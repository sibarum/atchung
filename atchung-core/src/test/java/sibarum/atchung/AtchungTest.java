package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtchungTest {

    private static final Topic<String> MSG = Topic.of("msg", String.class);

    @Test
    void inlineDeliversOnPublisherThreadToAllSubscribers() {
        Atchung bus = Atchung.create();
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        Thread publisher = Thread.currentThread();
        List<Thread> seen = new ArrayList<>();

        bus.subscribe(MSG, a::add);
        bus.subscribe(MSG, e -> {
            b.add(e);
            seen.add(Thread.currentThread());
        });

        bus.publish(MSG, "hello");

        assertEquals(List.of("hello"), a);
        assertEquals(List.of("hello"), b);
        assertSame(publisher, seen.get(0), "inline runs on the publisher thread");
    }

    @Test
    void closingSubscriptionStopsDelivery() {
        Atchung bus = Atchung.create();
        List<String> got = new ArrayList<>();
        Subscription sub = bus.subscribe(MSG, got::add);

        bus.publish(MSG, "one");
        assertTrue(sub.isActive());
        sub.close();
        assertFalse(sub.isActive());
        bus.publish(MSG, "two");

        assertEquals(List.of("one"), got);
        assertEquals(0, bus.subscriberCount(MSG));
    }

    @Test
    void pumpedDeliversOnDrainThreadInOrder() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> got = new ArrayList<>();
        pump.subscribe(MSG, got::add, 16, Backpressure.DROP_OLDEST);

        bus.publish(MSG, "a");
        bus.publish(MSG, "b");
        assertEquals(List.of(), got, "nothing delivered until drain()");
        assertTrue(pump.hasPending());

        int n = pump.drain();
        assertEquals(2, n);
        assertEquals(List.of("a", "b"), got);
        assertFalse(pump.hasPending());
    }

    @Test
    void dropOldestEvictsWhenFull() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> got = new ArrayList<>();
        pump.subscribe(MSG, got::add, 2, Backpressure.DROP_OLDEST);

        bus.publish(MSG, "1");
        bus.publish(MSG, "2");
        bus.publish(MSG, "3"); // evicts "1"
        pump.drain();

        assertEquals(List.of("2", "3"), got);
    }

    @Test
    void coalesceLatestKeepsOnlyNewest() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> got = new ArrayList<>();
        pump.subscribe(MSG, got::add, 1, Backpressure.COALESCE_LATEST);

        for (int i = 0; i < 100; i++) {
            bus.publish(MSG, "pos-" + i);
        }
        pump.drain();

        assertEquals(List.of("pos-99"), got, "only the latest survives");
    }

    @Test
    void pauseIsLossyForPushSubscribers() {
        Atchung bus = Atchung.create();
        List<String> got = new ArrayList<>();
        Subscription sub = bus.subscribe(MSG, got::add);

        bus.publish(MSG, "before");
        sub.pause();
        assertTrue(sub.isPaused());
        bus.publish(MSG, "during-1");
        bus.publish(MSG, "during-2");
        sub.resume();
        assertFalse(sub.isPaused());
        bus.publish(MSG, "after");

        assertEquals(List.of("before", "after"), got, "events during pause are lost, not buffered");
    }

    @Test
    void pauseIsLossyForPumpedSubscribers() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> got = new ArrayList<>();
        Subscription sub = pump.subscribe(MSG, got::add, 16, Backpressure.DROP_OLDEST);

        bus.publish(MSG, "a");
        sub.pause();
        bus.publish(MSG, "lost");
        sub.resume();
        bus.publish(MSG, "b");
        pump.drain();

        assertEquals(List.of("a", "b"), got, "paused mailbox is not filled");
    }

    @Test
    void asyncDeliversOnExecutor() throws Exception {
        Atchung bus = Atchung.create();
        ExecutorService exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "async-worker");
            t.setDaemon(true);
            return t;
        });
        try {
            CountDownLatch done = new CountDownLatch(1);
            List<String> threadName = new ArrayList<>();
            bus.subscribeAsync(MSG, e -> {
                threadName.add(Thread.currentThread().getName());
                done.countDown();
            }, exec);

            bus.publish(MSG, "x");
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals("async-worker", threadName.get(0));
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    void concurrentPublishersAllReachPumpedSubscriber() throws Exception {
        Atchung bus = Atchung.create();
        Topic<Integer> nums = Topic.of("nums", Integer.class);
        Pump pump = bus.pump();
        AtomicInteger sum = new AtomicInteger();
        pump.subscribe(nums, sum::addAndGet, 100_000, Backpressure.DROP_OLDEST);

        int threads = 8;
        int perThread = 1000;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            exec.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    bus.publish(nums, 1);
                }
                return null;
            });
        }
        start.countDown();
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));

        pump.drain();
        assertEquals(threads * perThread, sum.get(), "no events lost across concurrent publishers");
    }
}
