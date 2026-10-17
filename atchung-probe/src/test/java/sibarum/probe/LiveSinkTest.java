package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A live sink when the file is the aligned text format, not the CSV: the rows are the same rows, numbered the
 * same way, because the sink is fed from the one emission path whichever format the file is written in.
 */
class LiveSinkTest {

    private static final Path LOG = Path.of("target", "probe-test-trace.log");

    @Test
    void aSinkGetsNumberedRowsInTextModeTooInTheFilesOrder() throws IOException {
        assertFalse(Probe.CSV);
        List<String> lines = new ArrayList<>();
        List<Long> seqs = new ArrayList<>();
        LiveSink sink = (seq, mono, wall, thread, lane, kind, detail) -> {
            seqs.add(seq);
            lines.add(lane.key() + " [" + thread + "] " + kind + " " + detail);
            assertTrue(wall > 0, "the wall clock is there even though this file does not print it");
        };
        assertTrue(Probe.attach(sink));
        try {
            for (int i = 0; i < 10; i++) {
                Probe.mark(Lane.APP, "text.live", "n" + i);
            }
        } finally {
            Probe.detach(sink);
        }

        assertEquals(10, lines.size());
        for (int i = 1; i < seqs.size(); i++) {
            assertEquals(seqs.get(i - 1) + 1, seqs.get(i));
        }
        String file = Files.readString(LOG);
        int from = 0;
        for (String l : lines) {
            // The text line is "<time> <lane padded> [thread] kind detail"; the padding is the file's own.
            String tail = "[" + Thread.currentThread().getName() + "] " + l.substring(l.indexOf("] ") + 2);
            int at = file.indexOf(tail, from);
            assertTrue(at >= 0, "the file has the row the sink got, after the one before it: " + l);
            from = at + tail.length();
        }
    }
}
