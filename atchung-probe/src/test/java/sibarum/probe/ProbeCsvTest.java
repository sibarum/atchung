package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The correlation log, written for real.
 *
 * <p>Runs in a JVM of its own — the format is decided once in {@link Probe}'s static initialiser, so a single
 * process can only ever be one format, and this module's surefire starts a second one at
 * {@code -Dprobe.format=csv} (see the pom). Testing it any other way would be testing a different seam from the
 * one that ships.
 *
 * <p>What is pinned here is what makes the file <em>readable by a program</em> rather than merely produced.
 * A correlation log that is subtly malformed is worse than none: it is read with {@code sort}, {@code grep -n}
 * and {@code awk} during an investigation that is already confusing, and every one of those tools silently
 * gives wrong answers on a file whose rows do not line up with its lines.
 */
class ProbeCsvTest {

    private static final Path LOG = Path.of("target", "probe-test.csv");
    private static final String HEADER = "seq,t_mono_ns,t_wall,thread,lane,kind,detail";

    private static List<String> lines() throws IOException {
        Probe.dump();                       // the rollup must not land in the data file; see below
        return Files.readAllLines(LOG);
    }

    /** The rows for one kind, in file order. */
    private static List<String> rowsFor(String kind) throws IOException {
        return lines().stream().filter(l -> l.contains("," + kind + ",")).toList();
    }

    @Test
    void csvImpliesTracingAndStartsWithAHeader() throws IOException {
        assertTrue(Probe.ON);
        assertTrue(Probe.TRACE, "a correlation log holding only the slow spans is not one");

        Probe.mark(Lane.APP, "csv.header.probe", "x");
        assertEquals(HEADER, lines().get(0), "one header, first, so a reader needs no other convention");
    }

    @Test
    void everyEventIsExactlyOnePhysicalLine() throws IOException {
        int before = lines().size();
        // The case that ends a file's usefulness. RFC 4180 permits a raw newline inside a quoted field, and one
        // stack trace written that way makes every line tool describe fragments of events instead of events.
        Probe.mark(Lane.APP, "csv.multiline", "first\nsecond\r\nthird\rfourth");
        List<String> after = lines();

        assertEquals(before + 1, after.size(), "a detail with four line breaks in it is still one row");
        String row = after.get(after.size() - 1);
        assertTrue(row.contains("first second third fourth"), "and the content survives, flattened: " + row);
    }

    @Test
    void aFieldWithCommasOrQuotesIsQuotedTheWayReadersExpect() throws IOException {
        Probe.mark(Lane.APP, "csv.commas", "a,b");
        assertTrue(rowsFor("csv.commas").get(0).endsWith("\"a,b\""),
                "a comma in the detail must not read as a column break");

        Probe.mark(Lane.APP, "csv.quotes", "say \"hi\", twice");
        assertTrue(rowsFor("csv.quotes").get(0).endsWith("\"say \"\"hi\"\", twice\""),
                "RFC 4180 doubles an embedded quote; anything else and the field never closes");
    }

    @Test
    void seqIsGaplessSoThatLossIsDetectable() throws IOException {
        for (int i = 0; i < 20; i++) {
            Probe.mark(Lane.APP, "csv.seq", Integer.toString(i));
        }
        List<String> rows = rowsFor("csv.seq");
        assertEquals(20, rows.size());

        long previous = -1;
        for (String row : rows) {
            long seq = Long.parseLong(row.substring(0, row.indexOf(',')));
            if (previous >= 0) {
                // Not the sort key — but a gap here is the difference between "nothing happened" and "we did
                // not hear about it", which is exactly the ambiguity a stall hunt cannot afford.
                assertEquals(previous + 1, seq, "consecutive events must be consecutively numbered");
            }
            previous = seq;
        }
    }

    @Test
    void theMonotonicClockOnlyEverMovesForward() throws IOException {
        for (int i = 0; i < 20; i++) {
            Probe.mark(Lane.APP, "csv.clock", Integer.toString(i));
        }
        long previous = -1;
        for (String row : rowsFor("csv.clock")) {
            String[] f = row.split(",");
            long mono = Long.parseLong(f[1]);
            assertTrue(mono >= previous, "the sort key must never step backwards: " + row);
            previous = mono;
        }
    }

    @Test
    void everyRowHasTheColumnsTheHeaderPromises() throws IOException {
        Probe.count(Lane.APP, "csv.count", 7);
        try (Zone z = Probe.zone(Lane.APP, "csv.span")) {
            assertFalse(z == Zone.NONE);
        }
        Probe.opened(Lane.APP, "csv.thing", ProbeCsvTest.class);

        int columns = HEADER.split(",").length;
        for (String row : lines()) {
            if (row.equals(HEADER)) {
                continue;
            }
            // Split on commas outside quotes: the same thing a consumer does, so a row that breaks this breaks
            // them. Counting fields rather than eyeballing the row is the point — a trailing empty detail and a
            // missing column look identical until something counts.
            assertEquals(columns, fields(row), "wrong column count: " + row);
        }
    }

    @Test
    void theRollupStaysOutOfTheDataFile() throws IOException {
        Probe.dump();
        for (String row : lines()) {
            assertFalse(row.startsWith("probe:") || row.contains("  ..."),
                    "the summary is a table for a person; this file is a table for a program: " + row);
        }
    }

    /** Fields in one CSV row, counting only the commas that are outside quotes. */
    private static int fields(String row) {
        int n = 1;
        boolean quoted = false;
        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == ',' && !quoted) {
                n++;
            }
        }
        return n;
    }
}
