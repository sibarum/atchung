package sibarum.probe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
    void aFileWithCausesIsReadByItsHeader(@TempDir Path dir) throws IOException {
        // Every test above writes the layout from before causes, which must go on reading: old recordings are still
        // what people investigate. This is the layout the probe writes now, cause before detail.
        Path file = Files.writeString(dir.resolve("caused.csv"), "seq,t_mono_ns,t_wall,thread,lane,kind,cause,detail\n"
                + "1,1000,w,main,app,note,3f2a1.t07.4f2,\"a,b\"\n"
                + "2,2000,w,main,app,note,,plain\n");
        List<CsvView.Row> rows = CsvView.read(file);
        assertEquals("a,b", rows.get(0).detail());
        assertEquals("plain", rows.get(1).detail(), "an empty cause is an empty column, not a missing one");
    }

    /**
     * A small run as the probe writes it now: a timer firing twice, a click once, and a worker nobody wrapped. Each
     * firing writes to the tree; the click writes once and also logs.
     */
    private static Path causedRun(Path dir) throws IOException {
        String c = "seq,t_mono_ns,t_wall,thread,lane,kind,cause,detail\n";
        return Files.writeString(dir.resolve("caused-run.csv"), c
                + "1,0,w,main,app,cause.process,,3f2a1 pid=7\n"
                + "2,1000,w,main,app,cause.source,,t00 krono.transport\n"
                + "3,2000,w,main,app,cause.source,,i01 input.button\n"
                + "4,100000000,w,main,layout,mutation layout,3f2a1.t00.1,node=4\n"
                + "5,100000001,w,main,layout,mutation draw,3f2a1.t00.1,node=5\n"
                + "6,200000000,w,main,layout,mutation layout,3f2a1.t00.2,node=4\n"
                + "7,200000001,w,main,layout,mutation draw,3f2a1.t00.2,node=5\n"
                + "8,250000000,w,main,input,pointer.press,3f2a1.i01.9,LEFT\n"
                + "9,250000001,w,handler,layout,mutation layout,3f2a1.i01.9,node=8\n"
                + "10,300000000,w,worker,layout,mutation draw,,node=9\n"
                + "11,310000000,w,peer,layout,mutation draw,0bad1.c03.2,node=9\n");
    }

    @Test
    void rowsAreCountedByTheSourceOfTheirCauses(@TempDir Path dir) throws IOException {
        List<CsvView.Row> all = CsvView.read(causedRun(dir));
        List<CsvView.CauseGroup> groups = CsvView.byCause(all, CsvView.filter(all, "layout", null, null, null), true);

        // The timer's two firings are two causes and one source: the answer to "what writes so often?".
        CsvView.CauseGroup timer = groups.get(0);
        assertEquals("3f2a1.t00", timer.key());
        assertEquals("krono.transport", timer.name(), "named by the file's own registration row");
        assertEquals(2, timer.causes());
        assertEquals(4, timer.rows());
        assertEquals(Map.of("mutation layout", 2, "mutation draw", 2), timer.kinds());
        assertEquals(4 / 0.100000001, timer.perSecond(), 1e-3, "over its own span, first row to last");

        CsvView.CauseGroup click = groups.stream().filter(g -> g.key().equals("3f2a1.i01")).findFirst().orElseThrow();
        assertEquals(1, click.rows(), "the press itself is input, not layout: the filter came first");
        assertEquals("input.button", click.name());

        // A worker nobody wrapped is a named gap, not an anonymous row.
        assertTrue(groups.stream().anyMatch(g -> g.key().equals("- worker") && g.rows() == 1), groups.toString());
        // A cause from another process keeps its own prefix, and this file cannot name its source.
        assertTrue(groups.stream().anyMatch(g -> g.key().equals("0bad1.c03") && g.name().isEmpty()), groups.toString());
    }

    @Test
    void byCauseEachFiringIsItsOwnGroup(@TempDir Path dir) throws IOException {
        List<CsvView.Row> all = CsvView.read(causedRun(dir));
        List<CsvView.CauseGroup> groups = CsvView.byCause(all, all, false);
        assertTrue(groups.stream().anyMatch(g -> g.key().equals("3f2a1.t00.1") && g.rows() == 2 && g.causes() == 1));
        assertTrue(groups.stream().anyMatch(g -> g.key().equals("3f2a1.i01.9") && g.rows() == 2),
                "the press and the write it made are one cause");

        String table = CsvView.causes(all, all, false);
        assertTrue(table.startsWith("cause "), table);
        assertTrue(table.contains("krono.transport") && table.contains("(no cause)"), table);
    }

    @Test
    void aFileFromBeforeCausesIsAllUncaused(@TempDir Path dir) throws IOException {
        Path file = new Log().frame(0).row(1, "worker", "app", "save", "x").write(dir, "old.csv");
        List<CsvView.Row> rows = CsvView.read(file);
        assertTrue(rows.stream().allMatch(r -> r.cause().isEmpty()));
        List<CsvView.CauseGroup> groups = CsvView.byCause(rows, rows, true);
        assertEquals(List.of("- loop", "- worker"), groups.stream().map(CsvView.CauseGroup::key).sorted().toList());
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
