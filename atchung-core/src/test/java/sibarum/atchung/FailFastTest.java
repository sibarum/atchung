package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Backpressure#FAIL}: a full mailbox stops the process rather than losing an event.
 *
 * <p>The tests install {@link Fatal#THROW} first, for the obvious reason — the default policy halts the JVM,
 * and a suite that exercised it would take itself down. That substitution is also the thing worth checking
 * on its own: the policy runs <em>before</em> the throw, so a production process never reaches the throw at
 * all, and the throw is only what happens when a policy declines to stop.
 */
class FailFastTest {

    private static final Topic<String> EDGES = Topic.of("test.edges", String.class);

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
    void aFullMailboxThrowsRatherThanDroppingAnEvent() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> seen = new ArrayList<>();
        pump.subscribe(EDGES, seen::add, 2, Backpressure.FAIL);

        bus.publish(EDGES, "a");
        bus.publish(EDGES, "b");

        MailboxOverflow overflow = assertThrows(MailboxOverflow.class, () -> bus.publish(EDGES, "c"));

        assertEquals(EDGES, overflow.topic(), "the report names the channel that overflowed");
        assertEquals(2, overflow.capacity(), "and the bound it reached");
        assertEquals(0, overflow.delivered(), "and that nothing had ever drained — this consumer never ran");

        pump.drain();
        assertEquals(List.of("a", "b"), seen,
                "the queued events survive: FAIL refuses the new one, it does not evict an old one");
    }

    @Test
    void aConsumerThatKeepsUpNeverNoticesThePolicy() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> seen = new ArrayList<>();
        pump.subscribe(EDGES, seen::add, 2, Backpressure.FAIL);

        bus.publish(EDGES, "a");
        bus.publish(EDGES, "b");
        pump.drain();
        bus.publish(EDGES, "c");
        bus.publish(EDGES, "d");
        pump.drain();

        assertEquals(List.of("a", "b", "c", "d"), seen,
                "a consumer that keeps up never notices the policy at all");
    }

    @Test
    void theFatalPolicyRunsBeforeTheThrow() {
        AtomicReference<Throwable> reported = new AtomicReference<>();
        Atchung.onFatal(reported::set);

        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDGES, e -> { }, 1, Backpressure.FAIL);
        bus.publish(EDGES, "a");

        MailboxOverflow thrown = assertThrows(MailboxOverflow.class, () -> bus.publish(EDGES, "b"));

        assertNotNull(reported.get(), "the policy is consulted, and it is consulted first");
        assertSame(thrown, reported.get(), "with the same fault that is then thrown");
    }

    @Test
    void theDefaultPolicyForAPumpedSubscriptionIsToFail() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDGES, e -> { }, 1);   // no policy named

        bus.publish(EDGES, "a");

        assertThrows(MailboxOverflow.class, () -> bus.publish(EDGES, "b"),
                "losing events is a decision, so the overload that states no decision does not lose them");
    }

    @Test
    void theMessageSaysEnoughToActOnWithoutADebugger() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(EDGES, e -> { }, 1, Backpressure.FAIL);
        bus.publish(EDGES, "a");

        MailboxOverflow overflow = assertThrows(MailboxOverflow.class, () -> bus.publish(EDGES, "b"));

        String message = overflow.getMessage();
        assertTrue(message.contains("test.edges"), message);
        assertTrue(message.contains("capacity 1"), message);
    }

    @Test
    void theDefaultFatalPolicyIsToHalt() {
        Atchung.onFatal(null);
        assertSame(Fatal.HALT, Atchung.fatal(),
                "a process that has not chosen otherwise stops, rather than carrying on with lost data");
    }
}
