package sibarum.probe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogFileTest {

    @Test
    void nothingTouchesTheDiskUntilTheFirstLine(@TempDir Path tmp) {
        Path dir = tmp.resolve("logs");
        try (LogFile file = new LogFile(dir.resolve("app.log"), 1024, 3)) {
            assertFalse(Files.exists(dir), "reading leaves no mark");
        }
        assertFalse(Files.exists(dir));
    }

    @Test
    void theDirectoryAndTheFileAppearTogetherAtTheFirstWrite(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("a").resolve("b").resolve("app.log");
        try (LogFile file = new LogFile(log, 1024, 3)) {
            file.write("one\n", true);
        }
        assertEquals("one\n", Files.readString(log));
    }

    @Test
    void itAppendsToAFileThatIsAlreadyThere(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("app.log");
        Files.writeString(log, "before\n");
        try (LogFile file = new LogFile(log, 1024, 3)) {
            file.write("after\n", true);
        }
        assertEquals("before\nafter\n", Files.readString(log));
    }

    @Test
    void itRollsBySizeAndKeepsOnlyWhatItWasTold(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("app.log");
        try (LogFile file = new LogFile(log, 20, 2)) {
            for (int i = 1; i <= 6; i++) {
                file.write("line-" + i + "-xxxxxxxx\n", true);   // 16 bytes: one per file, two would not fit
            }
        }
        assertEquals("line-6-xxxxxxxx\n", Files.readString(log));
        assertEquals("line-5-xxxxxxxx\n", Files.readString(tmp.resolve("app.log.1")));
        assertEquals("line-4-xxxxxxxx\n", Files.readString(tmp.resolve("app.log.2")));
        assertFalse(Files.exists(tmp.resolve("app.log.3")), "keep=2 keeps two rolled files");
    }

    @Test
    void keepingNoHistoryJustStartsAgain(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("app.log");
        try (LogFile file = new LogFile(log, 20, 0)) {
            file.write("line-1-xxxxxxxx\n", true);
            file.write("line-2-xxxxxxxx\n", true);
        }
        assertEquals("line-2-xxxxxxxx\n", Files.readString(log));
        assertFalse(Files.exists(tmp.resolve("app.log.1")));
    }

    @Test
    void aLineLongerThanTheLimitIsWrittenWholeNotRefused(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("app.log");
        String big = "x".repeat(100) + "\n";
        try (LogFile file = new LogFile(log, 20, 1)) {
            file.write(big, true);
        }
        assertEquals(big, Files.readString(log));
    }

    @Test
    void aFileThatCannotBeWrittenIsGivenUpOnWithoutThrowing(@TempDir Path tmp) throws IOException {
        Path blocker = tmp.resolve("blocker");
        Files.writeString(blocker, "i am a file");
        // A directory cannot be created beneath a regular file.
        try (LogFile file = new LogFile(blocker.resolve("logs").resolve("app.log"), 1024, 1)) {
            file.write("one\n", true);
            file.write("two\n", true);
        }
        assertTrue(Files.isRegularFile(blocker));
    }

    @Test
    void closedBufferedLinesAreNotLost(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("app.log");
        LogFile file = new LogFile(log, 1024, 1);
        file.write("buffered\n", false);
        file.close();
        assertEquals("buffered\n", Files.readString(log));
    }
}
