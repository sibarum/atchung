package sibarum.probe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A process with no standard error of its own: what {@code System.err} is handed reaches the log, and what an
 * ended process's native standard error left behind is read in by the next start.
 */
class StandardErrorTest {

    private PrintStream real;

    @BeforeEach
    void keepTheRealOne() {
        real = System.err;
    }

    @AfterEach
    void putItBack() {
        System.setErr(real);
        Logging.reset();
    }

    private static LogConfig config(Path dir) {
        return new LogConfig("t", Mode.PACKAGED, Level.WARN, Level.INFO, Map.of(), dir, LogConfig.Format.TEXT,
                1024 * 1024, 2, null, List.of());
    }

    @Test
    void whatIsPrintedThenFlushedIsOneRecordInTheFileAtOnce(@TempDir Path dir) throws IOException {
        LogConfig c = Logging.configure(config(dir));
        Logging.captureStandardError(null);
        try (Logging.Capture capture = Logging.capture()) {
            new IllegalStateException("bus overflow").printStackTrace();
            System.err.flush();   // as Fatal.HALT does before it halts
            List<Logging.Entry> records = capture.records();
            assertEquals(1, records.size(), records.toString());
            assertEquals("stderr", records.get(0).logger());
            assertTrue(records.get(0).message().contains("bus overflow")
                    && records.get(0).message().contains("\tat "), records.get(0).message());
        }
        String file = Files.readString(c.logFile());
        assertTrue(file.contains("WARN  [") && file.contains("stderr - java.lang.IllegalStateException: bus overflow"),
                file);
    }

    @Test
    void theConsoleSinkStopsRatherThanWritingIntoItself(@TempDir Path dir) throws IOException {
        LogConfig c = Logging.configure(config(dir));
        Logging.captureStandardError(null);
        Log.of("app").warn("once");
        System.err.flush();
        String file = Files.readString(c.logFile());
        assertEquals(1, file.lines().filter(l -> l.contains("once")).count(), file);
        assertFalse(file.contains("stderr -"), file);
    }

    @Test
    void anEndedProcesssNativeErrorIsReadInAndDeleted(@TempDir Path dir) throws IOException {
        LogConfig c = Logging.configure(config(dir));
        long ended = 999_999_999L;   // no such process
        Path left = c.stderrFile(ended);
        Files.writeString(left, "Fatal error: Unhandled exception\nSegfault at 0x0\n");
        Path live = c.stderrFile(ProcessHandle.current().pid());
        Files.writeString(live, "still being written");
        Path empty = c.stderrFile(ended - 1);
        Files.createFile(empty);

        try (Logging.Capture capture = Logging.capture()) {
            Logging.recoverStandardError();
            assertTrue(capture.has(Level.ERROR, "process " + ended + " ended with this on its standard error:\n"
                    + "Fatal error: Unhandled exception\nSegfault at 0x0"), capture.records().toString());
            assertEquals(1, capture.records().size(), capture.records().toString());
        }
        assertFalse(Files.exists(left));
        assertFalse(Files.exists(empty));
        assertTrue(Files.exists(live), "a running process's file is its own");
    }
}
