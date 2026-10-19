package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import sibarum.probe.Lane;
import sibarum.probe.LiveSink;
import sibarum.probe.Probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rows a named mailbox writes on the {@code bus} lane — {@code enqueue}, {@code deliver}, {@code drop} — read
 * back through a {@link LiveSink}, which is fed the same rows the file is.
 *
 * <p>Runs in a JVM of its own, started profiling in the CSV format (see this module's pom): the probe decides
 * whether it is on in a static initialiser, so a suite cannot turn it on halfway.
 */
class MailboxRowsTest {

    private static final Set<String> MAILBOX_KINDS = Set.of("enqueue", "deliver", "drop");

    /** One row, as the sink was handed it. */
    private record Row(String thread, String kind, String detail) {
        @Override
        public String toString() {
            return "[" + thread + "] " + kind + " " + detail;
        }
    }

    private final List<Row> rows = new ArrayList<>();
    private final LiveSink sink = (seq, mono, wall, thread, lane, kind, detail) -> {
        if (lane == Lane.BUS) {
            synchronized (rows) {
                rows.add(new Row(thread, kind, detail));
            }
        }
    };
    private Fatal previous;

    @BeforeEach
    void attach() {
        assertTrue(Probe.ON, "this test runs in the profiling JVM");
        assertTrue(Probe.attach(sink));
        previous = Atchung.fatal();
        Atchung.onFatal(Fatal.THROW);
    }

    @AfterEach
    void detach() {
        Probe.detach(sink);
        Atchung.onFatal(previous);
    }

    /** The mailbox rows, in order, as {@code kind detail}. */
    private List<String> mailboxRows() {
        List<String> out = new ArrayList<>();
        synchronized (rows) {
            for (Row r : rows) {
                if (MAILBOX_KINDS.contains(r.kind())) {
                    out.add(r.kind() + " " + r.detail());
                }
            }
        }
        return out;
    }

    private Row only(String kind) {
        synchronized (rows) {
            List<Row> of = rows.stream().filter(r -> r.kind().equals(kind)).toList();
            assertEquals(1, of.size(), "one " + kind + " row: " + rows);
            return of.get(0);
        }
    }

    private static void runOn(String threadName, Runnable work) throws InterruptedException {
        Thread t = new Thread(work, threadName);
        t.start();
        t.join();
    }

    @Test
    void enqueueIsOnThePublishersThreadAndDeliverOnTheDrainers() throws InterruptedException {
        Topic<String> topic = Topic.of("rows.threads", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe("dev.example.Worker#onJob", topic, e -> { }, 8);

        runOn("publisher", () -> {
            bus.publish(topic, "a");
            bus.publish(topic, "b");
        });
        runOn("drainer", pump::drain);

        assertEquals(List.of(
                "enqueue dev.example.Worker#onJob 1",
                "enqueue dev.example.Worker#onJob 2",
                "deliver dev.example.Worker#onJob 2"), mailboxRows());
        synchronized (rows) {
            for (Row r : rows) {
                if (r.kind().equals("enqueue")) {
                    assertEquals("publisher", r.thread(), "the enqueue says who sent it: " + r);
                }
            }
        }
        assertEquals("drainer", only("deliver").thread());
    }

    @Test
    void theTopicRowsAreWrittenAsTheyWereBesideThem() {
        Topic<String> topic = Topic.of("rows.beside", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe("dev.example.Echo#onPing", topic, e -> { }, 8);

        bus.publish(topic, "a");
        pump.drain();

        assertEquals("1", only("rows.beside depth").detail());
        assertEquals("1", only("rows.beside batch").detail());
        assertEquals(List.of("enqueue dev.example.Echo#onPing 1", "deliver dev.example.Echo#onPing 1"),
                mailboxRows());
    }

    @Test
    void anUnnamedMailboxWritesNoneOfThem() {
        Topic<String> topic = Topic.of("rows.unnamed", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe(topic, e -> { }, 1, Backpressure.DROP_NEWEST);

        bus.publish(topic, "a");
        bus.publish(topic, "b");
        pump.drain();

        assertEquals(List.of(), mailboxRows());
        assertEquals("1", only("rows.unnamed dropped").detail(), "while the topic rows still are");
    }

    @Test
    void eachLossyPolicyNamesItselfInTheDrop() {
        Topic<String> topic = Topic.of("rows.drop", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe("m#newest", topic, e -> { }, 1, Backpressure.DROP_NEWEST);
        pump.subscribe("m#oldest", topic, e -> { }, 1, Backpressure.DROP_OLDEST);
        pump.subscribe("m#latest", topic, e -> { }, 1, Backpressure.COALESCE_LATEST);

        bus.publish(topic, "a");
        bus.publish(topic, "b");

        List<String> got = mailboxRows();
        // Fan-out order between the three mailboxes is the bus's business; each one's own rows are in order.
        assertEquals(List.of("enqueue m#newest 1", "drop m#newest DROP_NEWEST"), of(got, "m#newest"));
        assertEquals(List.of("enqueue m#oldest 1", "drop m#oldest DROP_OLDEST", "enqueue m#oldest 1"),
                of(got, "m#oldest"));
        assertEquals(List.of("enqueue m#latest 1", "drop m#latest COALESCE_LATEST", "enqueue m#latest 1"),
                of(got, "m#latest"));
    }

    @Test
    void aFailingMailboxWritesItsDropBeforeTheFault() {
        Topic<String> topic = Topic.of("rows.fail", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe("m#fail", topic, e -> { }, 1);

        bus.publish(topic, "a");
        assertThrows(MailboxOverflow.class, () -> bus.publish(topic, "b"));

        assertEquals(List.of("enqueue m#fail 1", "drop m#fail FAIL"), mailboxRows());
    }

    @Test
    void aFoldThatSupersedesIsAnEnqueueAndNotADrop() {
        Topic<String> topic = Topic.of("rows.fold", String.class);
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        pump.subscribe("m#fold", topic, e -> { }, 4, Backpressure.FAIL, e -> e);

        bus.publish(topic, "x");
        bus.publish(topic, "x");
        pump.drain();

        assertEquals(List.of("enqueue m#fold 1", "enqueue m#fold 1", "deliver m#fold 1"), mailboxRows());
    }

    private static List<String> of(List<String> rows, String mailbox) {
        return rows.stream().filter(r -> r.split(" ")[1].equals(mailbox)).toList();
    }
}
