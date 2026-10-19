package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The named forms of {@link Pump#subscribe}: a name changes what the probe writes and nothing about delivery. The
 * rows themselves are {@link MailboxRowsTest}'s, in a JVM started profiling; this one runs with the probe off, which
 * is how most processes run, and checks that a name costs such a process nothing it can see.
 */
class NamedMailboxTest {

    private static final Topic<String> EDGES = Topic.of("test.named", String.class);

    @Test
    void eachNamedFormDeliversAsItsUnnamedFormDoes() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        List<String> c = new ArrayList<>();
        pump.subscribe("dev.example.A#on", EDGES, a::add, 4);
        pump.subscribe("dev.example.B#on", EDGES, b::add, 4, Backpressure.DROP_NEWEST);
        pump.subscribe("dev.example.C#on", EDGES, c::add, 4, Backpressure.FAIL, e -> e);

        bus.publish(EDGES, "x");
        bus.publish(EDGES, "y");
        bus.publish(EDGES, "x");
        assertEquals(8, pump.drain(), "3 out of each plain mailbox, and 2 cells out of the folding one");

        assertEquals(List.of("x", "y", "x"), a);
        assertEquals(List.of("x", "y", "x"), b);
        assertEquals(List.of("y", "x"), c, "the fold still folds: the second x superseded the first");
    }

    @Test
    void aNullNameIsAnUnnamedMailbox() {
        Atchung bus = Atchung.create();
        Pump pump = bus.pump();
        List<String> seen = new ArrayList<>();
        pump.subscribe(null, EDGES, seen::add, 4);

        bus.publish(EDGES, "x");
        pump.drain();
        assertEquals(List.of("x"), seen);
    }

    @Test
    void aNameARowCouldNotCarryIsRefused() {
        Pump pump = Atchung.create().pump();
        assertThrows(IllegalArgumentException.class, () -> pump.subscribe("", EDGES, e -> { }, 4),
                "a blank name would leave the row's detail starting with its number");
        assertThrows(IllegalArgumentException.class, () -> pump.subscribe("two words", EDGES, e -> { }, 4),
                "a reader splits the name off at the first space");
    }
}
