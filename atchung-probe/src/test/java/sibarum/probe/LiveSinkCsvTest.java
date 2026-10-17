package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A live sink against the file, in the format the file can be compared field for field: the correlation CSV.
 *
 * <p>Runs in the CSV JVM beside {@link ProbeCsvTest} (see the pom). The claim being pinned is the one the flight
 * recorder is built on: whatever a sink is given is exactly what the file got — no row more, no row fewer, in the
 * same order, under the same numbers — even with several threads emitting at once, which is the only case in
 * which "the same order" says anything.
 */
class LiveSinkCsvTest {

    private static final Path LOG = Path.of("target", "probe-test.csv");

    /** A row as a sink saw it, with the fields rendered the way the file renders them. */
    record Row(long seq, long mono, long wallNs, String thread, String lane, String kind, String detail) {

        String asCsv() {
            Instant wall = Instant.ofEpochSecond(wallNs / 1_000_000_000L, wallNs % 1_000_000_000L);
            return seq + "," + mono + "," + wall + "," + thread + "," + lane + "," + kind + "," + detail;
        }
    }

    static final class Recording implements LiveSink {
        final List<Row> rows = new ArrayList<>();

        @Override
        public void row(long seq, long tMonoNs, long tWallNs, String thread, Lane lane, String kind,
                        String detail) {
            rows.add(new Row(seq, tMonoNs, tWallNs, thread, lane.key(), kind, detail));
        }
    }

    @Test
    void aSinkReceivesExactlyTheRowsTheFileReceivesInTheSameOrderUnderTheSameSeq() throws Exception {
        Recording sink = new Recording();
        assertTrue(Probe.attach(sink));
        int threads = 4;
        int each = 500;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            Thread w = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < each; i++) {
                    Probe.mark(Lane.APP, "live.mark", id + "." + i);
                    Probe.count(Lane.BUS, "live.count", i);
                    try (Zone z = Probe.zone(Lane.FRAME, "live.span")) {
                        assertTrue(z != Zone.NONE);
                    }
                }
            }, "live-" + t);
            workers.add(w);
            w.start();
        }
        go.countDown();
        for (Thread w : workers) {
            w.join();
        }
        Probe.detach(sink);

        List<Row> got = sink.rows;
        assertTrue(got.size() >= threads * each * 3, "every row the workers emitted, at least: " + got.size());
        for (int i = 1; i < got.size(); i++) {
            assertEquals(got.get(i - 1).seq() + 1, got.get(i).seq(), "a sink attached throughout sees no gap");
            assertTrue(got.get(i).mono() >= got.get(i - 1).mono(), "t_mono_ns ascends in seq order");
        }

        // The file, over the same span of seq: every row, in file order, must be the sink's row rendered.
        long first = got.get(0).seq();
        long last = got.get(got.size() - 1).seq();
        List<String> file = new ArrayList<>();
        for (String line : Files.readAllLines(LOG)) {
            int comma = line.indexOf(',');
            if (comma <= 0 || !Character.isDigit(line.charAt(0))) {
                continue;                                   // the header
            }
            long seq = Long.parseLong(line.substring(0, comma));
            if (seq >= first && seq <= last) {
                file.add(line);
            }
        }
        List<String> expected = got.stream().map(Row::asCsv).toList();
        assertEquals(expected, file, "the sink and the file disagree");
    }

    @Test
    void aDetailIsHandedOverAsItsValueNotItsEncoding() {
        Recording sink = new Recording();
        Probe.attach(sink);
        try {
            Probe.mark(Lane.APP, "live.raw", "a,\"b\"\nc");
            Probe.mark(Lane.APP, "live.null", null);
        } finally {
            Probe.detach(sink);
        }
        assertEquals("a,\"b\"\nc", sink.rows.get(0).detail(), "quoting and flattening are the file's concern");
        assertEquals("", sink.rows.get(1).detail(), "an absent detail is empty, as in the file");
    }

    @Test
    void aSinkThatThrowsCostsItsOwnRowAndNothingElse() {
        Recording healthy = new Recording();
        List<Long> seen = new ArrayList<>();
        LiveSink flaky = (seq, mono, wall, thread, lane, kind, detail) -> {
            if (kind.equals("live.flaky") && detail.equals("boom")) {
                throw new IllegalStateException("a sink with a bug in it");
            }
            if (kind.equals("live.flaky")) {
                seen.add(seq);
            }
        };
        Probe.attach(flaky);                                // first, so a throw would starve the one after it
        Probe.attach(healthy);
        long failuresBefore = Probe.liveSinkFailures();
        try {
            assertDoesNotThrow(() -> {
                Probe.mark(Lane.APP, "live.flaky", "before");
                Probe.mark(Lane.APP, "live.flaky", "boom");
                Probe.mark(Lane.APP, "live.flaky", "boom");
                Probe.mark(Lane.APP, "live.flaky", "after");
            }, "a sink's exception must never reach the call site");
        } finally {
            Probe.detach(flaky);
            Probe.detach(healthy);
        }

        assertEquals(4, healthy.rows.size(), "the other sinks still get every row");
        assertEquals(2, Probe.liveSinkFailures() - failuresBefore, "every dropped row is counted");
        assertEquals(2, seen.size(), "the throwing sink stays attached and gets the rows after");
        assertEquals(3, seen.get(1) - seen.get(0), "and the rows it lost show up to it as a gap in seq");
    }

    @Test
    void attachingTwiceAttachesOnceAndDetachStopsDelivery() {
        Recording sink = new Recording();
        Probe.attach(sink);
        Probe.attach(sink);
        Probe.mark(Lane.APP, "live.once", "x");
        Probe.detach(sink);
        Probe.mark(Lane.APP, "live.once", "y");
        assertEquals(1, sink.rows.size());
    }

    @Test
    void aSinkThatEmitsDoesNotRecurseIntoItself() throws IOException {
        List<String> kinds = new ArrayList<>();
        LiveSink chatty = (seq, mono, wall, thread, lane, kind, detail) -> {
            kinds.add(kind);
            Probe.mark(Lane.APP, "live.echo", kind);       // breaks the contract; must not loop
        };
        Probe.attach(chatty);
        try {
            Probe.mark(Lane.APP, "live.origin", "x");
        } finally {
            Probe.detach(chatty);
        }
        assertEquals(List.of("live.origin"), kinds);
        assertTrue(Files.readString(LOG).contains(",live.echo,live.origin"), "the nested row still reaches the file");
    }
}
