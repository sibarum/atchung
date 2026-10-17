package sibarum.probe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reading a correlation log back.
 *
 * <p>The verdict is the thing under test. A gap is not a finding on its own — a render-on-demand loop is
 * <em>supposed</em> to go quiet — so a tool that merely lists silences would report every idle window as a
 * hang and be ignored within a day. What has to be right is the judgement: a gap the preceding park accounts
 * for is expected, and one it does not is a stall.
 *
 * <p>Written against synthetic logs rather than a recorded run, so each case states exactly one situation and
 * a failure names it.
 */
class CsvViewTest {

    private static final String HEADER = "seq,t_mono_ns,t_wall,thread,lane,kind,detail";

    /** A log built row by row, so a test can say precisely what the loop did and when. */
    private static final class Log {
        private final StringBuilder sb = new StringBuilder(HEADER + "\n");
        private long seq = 1;

        Log row(long millis, String thread, String lane, String kind, String detail) {
            sb.append(seq++).append(',').append(millis * 1_000_000L).append(',')
                    .append("2026-09-02T05:00:").append(String.format("%06.3f", millis / 1000.0)).append('Z')
                    .append(',').append(thread).append(',').append(lane).append(',').append(kind)
                    .append(',').append(detail).append('\n');
            return this;
        }

        Log frame(long millis) {
            return row(millis, "loop", "frame", "frame.present", "#" + seq);
        }

        Log park(long millis, String budget) {
            return row(millis, "loop", "frame", "loop.park", budget);
        }

        Log skipSeq() {
            seq++;                      // a row that was dropped rather than written
            return this;
        }

        Path write(Path dir, String name) throws IOException {
            return Files.writeString(dir.resolve(name), sb.toString());
        }
    }

    @Test
    void aGapTheParkAccountsForIsNotAFinding(@TempDir Path dir) throws IOException {
        Path file = new Log()
                .frame(0).park(0, "forever")
                .frame(5_000).park(5_000, "0ms")
                .write(dir, "idle.csv");

        String out = CsvView.gaps(CsvView.read(file), 100);
        // Five seconds of silence on a window nobody was looking at. Reporting this as a hang is how a tool
        // teaches its user to stop reading it.
        assertTrue(out.contains("expected"), out);
        assertFalse(out.contains("STALL"), out);
    }

    @Test
    void aGapNothingAnnouncedIsAStall(@TempDir Path dir) throws IOException {
        Path file = new Log()
                .frame(0)
                .row(1, "worker", "app", "save", "writing 40MB")
                .frame(900)
                .write(dir, "stall.csv");

        String out = CsvView.gaps(CsvView.read(file), 100);
        assertTrue(out.contains("STALL"), out);
        assertTrue(out.contains("nothing said the loop was going to sleep"), out);
        // The row before the silence, which in a stall is usually the cause — the whole reason this is one
        // file in time order rather than several.
        assertTrue(out.contains("writing 40MB"), "the suspect has to be named: " + out);
    }

    @Test
    void aParkTooShortToCoverTheGapIsAlsoAStall(@TempDir Path dir) throws IOException {
        Path file = new Log()
                .frame(0).park(0, "16ms")
                .frame(800)
                .write(dir, "overslept.csv");

        String out = CsvView.gaps(CsvView.read(file), 100);
        // The subtle one: the loop did say it was sleeping, and then failed to come back on time. A tool that
        // only asked "was there a park?" would call this healthy.
        assertTrue(out.contains("STALL"), out);
        assertTrue(out.contains("does not cover it"), out);
    }

    /** The rule over rows that never were a file: what a live sink keeps in memory reads the same verdicts. */
    @Test
    void theGapRuleJudgesRowsHeldInMemory() {
        record Held(long monoNanos, String kind, String detail) implements CsvView.Stamped {
        }
        long ms = 1_000_000L;
        List<Held> rows = List.of(
                new Held(0, "frame.present", "#1"), new Held(1 * ms, "loop.park", "forever"),
                new Held(500 * ms, "frame.present", "#2"), new Held(501 * ms, "loop.park", "16ms"),
                new Held(600 * ms, "save", "writing 40MB"),
                new Held(900 * ms, "frame.present", "#3"),
                new Held(910 * ms, "frame.present", "#4"));

        List<CsvView.Gap<Held>> gaps = CsvView.gapsOf(rows, 100 * ms);

        assertEquals(2, gaps.size(), gaps.toString());
        assertEquals(CsvView.Verdict.EXPECTED, gaps.get(0).verdict());
        assertEquals(500 * ms, gaps.get(0).nanos());
        assertEquals(CsvView.Verdict.SHORT_PARK, gaps.get(1).verdict());
        assertTrue(gaps.get(1).verdict().stall());
        assertEquals("writing 40MB", gaps.get(1).last().detail());
        assertEquals("16ms", gaps.get(1).park().detail());
    }

    /**
     * Frame to frame is the park plus the next frame's work. A loop idling on a 200ms floor shows 200.7ms gaps, and
     * the 0.7ms the park does not cover is the next frame being drawn, not a stall — found on the gui demo, where
     * the first version of this rule called every idle frame a stall.
     */
    @Test
    void theNextFramesOwnWorkIsNotAStall(@TempDir Path dir) throws IOException {
        Path file = new Log()
                .frame(0).park(0, "200ms")
                .row(201, "main", "frame", "frame", "0.70ms")
                .frame(201)
                .write(dir, "idle-floor.csv");

        String out = CsvView.gaps(CsvView.read(file), 100);
        assertTrue(out.contains("expected - parked on 200ms"), out);
        assertFalse(out.contains("STALL"), out);
    }

    @Test
    void aRunWithNoGapsSaysSoRatherThanPrintingNothing(@TempDir Path dir) throws IOException {
        Path file = new Log().frame(0).frame(16).frame(32).write(dir, "smooth.csv");
        String out = CsvView.gaps(CsvView.read(file), 100);
        assertTrue(out.startsWith("no gaps"), out);
    }

    @Test
    void rowsComeBackInTimeOrderNotInFileOrder(@TempDir Path dir) throws IOException {
        // Two threads whose numbering disagrees with the clock, which is exactly what seq is not for.
        Path file = Files.writeString(dir.resolve("threads.csv"), HEADER + "\n"
                + "1,2000000,w,worker,app,late,\n"
                + "2,1000000,l,loop,frame,frame.present,#1\n");

        List<CsvView.Row> rows = CsvView.read(file);
        assertEquals("frame.present", rows.get(0).kind(), "the clock says what happened first, not the counter");
        assertEquals("late", rows.get(1).kind());
    }

    @Test
    void missingRowsAreReportedRatherThanQuietlySkipped(@TempDir Path dir) throws IOException {
        Path file = new Log().frame(0).skipSeq().frame(16).write(dir, "lossy.csv");
        // Every other answer this tool gives is computed from the rows that are present. A reader not told
        // about the missing ones is being invited to conclude something from a hole.
        assertTrue(CsvView.loss(CsvView.read(file)).contains("1 row(s) missing"));
        assertTrue(CsvView.loss(CsvView.read(new Log().frame(0).frame(16).write(dir, "whole.csv"))).isEmpty());
    }

    @Test
    void aProjectionKeepsOnlyWhatWasAskedFor(@TempDir Path dir) throws IOException {
        Path file = new Log()
                .frame(0)
                .row(1, "worker", "input", "pointer.move", "10,10")
                .row(2, "worker", "input", "pointer.press", "LEFT")
                .row(3, "loop", "layout", "layout.publish", "v4")
                .write(dir, "mixed.csv");
        List<CsvView.Row> rows = CsvView.read(file);

        assertEquals(2, CsvView.filter(rows, "input", null, null, null).size(), "by lane");
        assertEquals(1, CsvView.filter(rows, null, "press", null, null).size(), "by kind, as a substring");
        assertEquals(2, CsvView.filter(rows, null, null, "worker", null).size(), "by thread");
        assertEquals(1, CsvView.filter(rows, null, null, null, "v4").size(), "by anything in the row");
        // Combined rather than alternative: an investigation narrows, and each option has to be an "and".
        assertEquals(1, CsvView.filter(rows, "input", "move", "worker", null).size());
        assertEquals(4, CsvView.filter(rows, null, null, null, null).size(), "and nothing asked keeps everything");
    }

    @Test
    void aFilterMatchesRegardlessOfCase(@TempDir Path dir) throws IOException {
        Path file = new Log().row(0, "Loop", "FRAME", "Frame.Present", "#1").write(dir, "case.csv");
        List<CsvView.Row> rows = CsvView.read(file);
        // The producer chose the casing; the person at the terminal should not have to guess it.
        assertEquals(1, CsvView.filter(rows, "frame", "present", "loop", null).size());
    }

    @Test
    void aQuotedDetailIsReadBackTheWayItWasWritten(@TempDir Path dir) throws IOException {
        Path file = Files.writeString(dir.resolve("quoted.csv"), HEADER + "\n"
                + "1,1000,w,main,app,note,\"a,b and \"\"quoted\"\"\"\n");
        // The reader honours the writer's own rules; anything else and a detail with a comma in it silently
        // becomes an extra column and shifts every field after it.
        assertEquals("a,b and \"quoted\"", CsvView.read(file).get(0).detail());
    }

    @Test
    void aTornLastLineIsSkippedRatherThanFatal(@TempDir Path dir) throws IOException {
        // The normal shape of a log from the run this facility is for: one that was killed mid-write.
        Path file = Files.writeString(dir.resolve("torn.csv"), HEADER + "\n"
                + "1,1000,l,loop,frame,frame.present,#1\n"
                + "2,2000,l,loop,fra");
        assertEquals(1, CsvView.read(file).size(), "the whole rows are still readable");
    }
}
