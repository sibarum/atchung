package sibarum.probe;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A live sink in a process that is not profiling. Runs in a JVM of its own started with the probe off (see the
 * pom): the recorder registers unconditionally, and that must cost nothing and deliver nothing.
 */
class LiveSinkOffTest {

    @Test
    void attachingWhileOffIsHarmlessAndNothingArrives() {
        assertFalse(Probe.ON);
        int[] rows = {0};
        LiveSink sink = (seq, mono, wall, thread, lane, kind, detail) -> rows[0]++;
        assertFalse(Probe.attach(sink), "attach says nothing will arrive");
        Probe.mark(Lane.APP, "off.live", "x");
        Probe.count(Lane.APP, "off.live");
        Probe.detach(sink);
        assertEquals(0, rows[0]);
        assertEquals(0L, Probe.liveSinkFailures());
    }
}
