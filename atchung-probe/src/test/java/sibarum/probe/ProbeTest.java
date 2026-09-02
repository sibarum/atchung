package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The probe, running for real.
 *
 * <p>Whether the probe is on is decided once, in {@link Probe}'s static initialiser, from a property that has
 * to exist before the class is first touched. That is not something a test can arrange after the fact — so
 * this module's surefire configuration starts the test JVM with {@code -Dprobe=all} and a log file under
 * {@code target/}, and these tests read the file back. Testing the seam any other way would mean testing a
 * different seam from the one that ships.
 */
class ProbeTest {

    private static final Path LOG = Path.of("target", "probe-test.log");

    private static String log() throws IOException {
        return Files.readString(LOG);
    }

    @Test
    void theSwitchIsOnAndEveryLaneIsSelected() {
        assertTrue(Probe.ON, "surefire should have started this JVM with -Dprobe=all");
        for (Lane lane : Lane.values()) {
            assertTrue(lane.on(), lane.key() + " should be selected by 'all'");
        }
    }

    @Test
    void aSpanIsCountedAndItsSelfTimeExcludesItsChildren() {
        for (int i = 0; i < 5; i++) {
            try (Zone outer = Probe.zone(Lane.APP, "selftime-outer")) {
                busy(2_000_000L);
                try (Zone inner = Probe.zone(Lane.APP, "selftime-inner")) {
                    busy(8_000_000L);
                }
            }
        }
        Probe.dump();
        String out = assertDumped();
        assertTrue(out.contains("selftime-outer"), out);
        assertTrue(out.contains("selftime-inner"), out);
        // The outer span's own self time is the part not spent in the inner one, so it is the smaller of the
        // two despite the outer span being the longer. That inversion is the entire point of the column.
        assertTrue(selfNanos(out, "selftime-outer") < selfNanos(out, "selftime-inner"),
                "outer self should be below inner self:\n" + out);
    }

    @Test
    void aCounterKeepsItsPeakNotJustItsMean() {
        Probe.count(Lane.BUS, "depth", 1);
        Probe.count(Lane.BUS, "depth", 1);
        Probe.count(Lane.BUS, "depth", 4096);
        Probe.dump();
        String row = row(assertDumped(), "depth");
        // n, sum, mean, max — the peak is the last column, and it is the number that says events were dropped.
        assertTrue(row.endsWith("4096"), row);
    }

    @Test
    void anUnclosedResourceIsStillLiveInTheLedger() {
        Object kept = new Object();
        Object released = new Object();
        Probe.opened(Lane.GPU, "TestHandle", kept);
        Probe.opened(Lane.GPU, "TestHandle", released);
        Probe.closed(Lane.GPU, "TestHandle", released);
        Probe.dump();
        String row = row(assertDumped(), "TestHandle");
        // opened, closed, LIVE
        assertTrue(row.matches(".*\\s+2\\s+1\\s+1$"), row);
        assertEquals(kept, kept);   // the ledger holds it; so does this test, deliberately
    }

    @Test
    void aZoneOnAnUnselectedLaneRecordsNothingAndDoesNotThrow() {
        Lane.SHADER.on = false;
        try (Zone z = Probe.zone(Lane.SHADER, "not-recorded")) {
            busy(1_000_000L);
        }
        Lane.SHADER.on = true;
        Probe.dump();
        assertFalse(assertDumped().contains("not-recorded"));
    }

    /** The last rollup in the log — every test appends one, and only the newest describes this test. */
    private static String assertDumped() {
        try {
            String all = log();
            int last = all.lastIndexOf("=== probe:");
            assertTrue(last >= 0, "no rollup was written to " + LOG.toAbsolutePath());
            return all.substring(last);
        } catch (IOException e) {
            throw new AssertionError("probe log unreadable", e);
        }
    }

    private static String row(String report, String name) {
        for (String line : report.split("\\R")) {
            if (line.contains(name)) {
                return line.trim();
            }
        }
        throw new AssertionError("no row for '" + name + "' in:\n" + report);
    }

    /** The self column is the last on a span row. Parsed back out rather than reached for internally. */
    private static double selfNanos(String report, String name) {
        String[] cells = row(report, name).split("\\s+");
        return nanos(cells[cells.length - 1]);
    }

    private static double nanos(String formatted) {
        if (formatted.endsWith("ns")) {
            return Double.parseDouble(formatted.substring(0, formatted.length() - 2));
        }
        if (formatted.endsWith("us")) {
            return Double.parseDouble(formatted.substring(0, formatted.length() - 2)) * 1e3;
        }
        if (formatted.endsWith("ms")) {
            return Double.parseDouble(formatted.substring(0, formatted.length() - 2)) * 1e6;
        }
        return Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e9;
    }

    /** Spin for {@code nanos}. Sleeping would measure the scheduler; this measures the clock. */
    private static void busy(long nanos) {
        long until = System.nanoTime() + nanos;
        while (System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }
}
