package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import sibarum.probe.Cause;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Sender#paced}: a reader slower than its sender paces the sender rather than overflowing (vexelray-framework
 * components.md, section 15, MainFrame FN-9). The sender's thread waits for room; the reader's capacity is the window.
 */
class PacedSenderTest {

    private static final Topic<Integer> EDGES = Topic.of("test.paced.edges", Integer.class);
    private static final Cause.Source SOURCE = Cause.source("test.paced.source", Cause.Kind.IO);

    @Test
    void aSenderFasterThanItsReaderWaitsAndLosesNothing() throws Exception {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Integer> heard = new CopyOnWriteArrayList<>();
        tick.inbox("slow").subscribe(EDGES, heard::add, 4);
        Sender<Integer> sender = Sender.paced(bus, EDGES, SOURCE);

        int n = 200;
        Thread source = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                sender.send(i);
            }
        }, "test-paced-source");
        source.start();
        long until = System.nanoTime() + 10_000_000_000L;
        while (heard.size() < n && System.nanoTime() < until) {
            tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);
            Thread.sleep(1);
        }
        source.join(1000);

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            expected.add(i);
        }
        assertEquals(expected, heard, "every message, in order, through a mailbox of four");
        assertFalse(source.isAlive());
    }

    @Test
    void anInterruptEndsTheWaitAndTheMessageIsNotSent() throws Exception {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Integer> heard = new CopyOnWriteArrayList<>();
        tick.inbox("never").subscribe(EDGES, heard::add, 2);
        Sender<Integer> sender = Sender.paced(bus, EDGES, SOURCE);
        AtomicInteger sent = new AtomicInteger();
        boolean[] interrupted = new boolean[1];

        Thread source = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                sender.send(i);
                if (Thread.currentThread().isInterrupted()) {
                    interrupted[0] = true;
                    return;
                }
                sent.incrementAndGet();
            }
        }, "test-paced-stopped");
        source.start();
        long until = System.nanoTime() + 5_000_000_000L;
        while (sent.get() < 2 && System.nanoTime() < until) {
            Thread.sleep(1);
        }
        Thread.sleep(50);
        assertEquals(2, sent.get(), "two fit, and the third waits: nothing has drained");
        assertTrue(source.isAlive());
        source.interrupt();
        source.join(1000);
        assertFalse(source.isAlive(), "the interrupt ended the wait");
        assertTrue(interrupted[0], "with the flag set again");

        tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);
        assertEquals(List.of(0, 1), heard, "the message it was waiting to send was not sent");
    }

    @Test
    void aClosedReaderNoLongerHoldsTheSender() throws Exception {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Subscription sub = tick.inbox("goes").subscribe(EDGES, e -> { }, 1);
        Sender<Integer> sender = Sender.paced(bus, EDGES, SOURCE);
        sender.send(0);
        Thread source = new Thread(() -> sender.send(1), "test-paced-closed");
        source.start();
        Thread.sleep(50);
        assertTrue(source.isAlive(), "waiting for the one slot");
        sub.close();
        source.join(1000);
        assertFalse(source.isAlive(), "closing the reader woke it");
    }
}
