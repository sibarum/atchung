package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The ID and its sources: what a cause says about itself, and that it never outlives the work it was entered for. */
class CauseTest {

    @Test
    void aCauseSaysWhichProcessSourceAndSequenceItIs() {
        Cause.Source timer = Cause.source("cause-test.timer", Cause.Kind.TIMER);
        long a = timer.next();
        long b = timer.next();

        assertEquals(Cause.process(), Cause.processOf(a));
        assertEquals(Cause.Kind.TIMER, Cause.kindOf(a));
        assertEquals(timer.index(), Cause.sourceOf(a));
        assertEquals(Cause.sequenceOf(a) + 1, Cause.sequenceOf(b), "one source's causes differ by sequence");
        assertEquals(0L, Cause.sequenceOf(timer.defaultCause()), "and its default is sequence 0");
        assertNotEquals(Cause.NONE, timer.defaultCause(), "which is still a cause: the process prefix is never 0");
    }

    @Test
    void aCauseIsWrittenAsTextAndReadBack() {
        Cause.Source input = Cause.source("cause-test.input", Cause.Kind.INPUT);
        long cause = input.next();
        String text = Cause.format(cause);

        assertTrue(text.matches("[0-9a-f]{5}\\.i\\d{2,4}\\.[0-9a-f]+"), text);
        assertTrue(text.contains("." + input.key() + "."), "the source's key is in the middle: " + text);
        assertEquals(cause, Cause.parse(text));
        assertEquals("", Cause.format(Cause.NONE));
        assertEquals(Cause.NONE, Cause.parse(""));
        assertEquals(Cause.NONE, Cause.parse("not a cause"));
    }

    @Test
    void aSourceAskedForTwiceIsOneSource() {
        Cause.Source a = Cause.source("cause-test.same", Cause.Kind.IO);
        Cause.Source b = Cause.source("cause-test.same", Cause.Kind.IO);
        Cause.Source other = Cause.source("cause-test.same", Cause.Kind.TIMER);

        assertSame(a, b);
        assertNotEquals(a.index(), other.index(), "one name under two kinds is two sources");
    }

    @Test
    void causesAreDistinctAcrossSources() {
        Set<Long> seen = new HashSet<>();
        for (Cause.Kind kind : Cause.Kind.values()) {
            Cause.Source s = Cause.source("cause-test.distinct", kind);
            for (int i = 0; i < 100; i++) {
                assertTrue(seen.add(s.next()));
            }
        }
    }

    @Test
    void withinPutsBackTheCauseItFound() {
        Cause.Source s = Cause.source("cause-test.within", Cause.Kind.COMMAND);
        long outer = s.next();
        long inner = s.next();

        Cause.within(outer, () -> {
            assertEquals(outer, Cause.current());
            Cause.within(inner, () -> assertEquals(inner, Cause.current()));
            assertEquals(outer, Cause.current(), "the inner cause ended with its body");
        });
        assertEquals(Cause.NONE, Cause.current());

        assertThrows(IllegalStateException.class, () -> Cause.within(outer, () -> {
            throw new IllegalStateException("body failed");
        }));
        assertEquals(Cause.NONE, Cause.current(), "and a body that throws takes its cause with it");
    }

    @Test
    void beginRunsInTheSourcesNextCause() {
        Cause.Source s = Cause.source("cause-test.begin", Cause.Kind.INPUT);
        long[] seen = new long[2];
        Cause.begin(s, () -> seen[0] = Cause.current());
        Cause.begin(s, () -> seen[1] = Cause.current());

        assertEquals(s.index(), Cause.sourceOf(seen[0]));
        assertEquals(Cause.sequenceOf(seen[0]) + 1, Cause.sequenceOf(seen[1]));
    }

    @Test
    void enterAndExitArePaired() {
        long cause = Cause.source("cause-test.enter", Cause.Kind.MAILBOX).next();
        long previous = Cause.enter(cause);
        try {
            assertEquals(cause, Cause.current());
        } finally {
            Cause.exit(previous);
        }
        assertEquals(Cause.NONE, Cause.current());
    }
}
