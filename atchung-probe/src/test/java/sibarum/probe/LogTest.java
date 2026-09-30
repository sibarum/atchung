package sibarum.probe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The logger and the delivery behind it: levels, per-logger overrides, formatting, capture, the file, and the
 * discipline that a disabled call builds nothing.
 */
class LogTest {

    @AfterEach
    void forgetEverything() {
        Logging.reset();
    }

    private static LogConfig config(Mode mode, Level console, Level file, Map<String, Level> overrides, Path dir,
                                    LogConfig.Format format) {
        return new LogConfig("t", mode, console, file, overrides, dir, format, 1024 * 1024, 2, null, List.of());
    }

    /** Runs {@code body} with standard error captured, and returns what was written to it. */
    private static String console(Runnable body) {
        PrintStream real = System.err;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setErr(new PrintStream(bytes, true, StandardCharsets.UTF_8));
        try {
            body.run();
        } finally {
            System.setErr(real);
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void loggersAreInterned() {
        assertSame(Log.of("x.y"), Log.of("x.y"));
        assertEquals("LogTest", Log.of(LogTest.class).name());
    }

    @Test
    void theConsoleThresholdFiltersAndTheLineIsWhatWasAsked() {
        Logging.configure(config(Mode.DEV, Level.INFO, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        Log log = Log.of("gui.frame");
        String out = console(() -> {
            log.debug("hidden");
            log.info("window {} of {}", 2, 3);
            log.warn("careful");
        });
        assertFalse(out.contains("hidden"), out);
        assertTrue(out.contains("INFO  [") && out.contains("gui.frame - window 2 of 3"), out);
        assertTrue(out.contains("WARN  [") && out.contains("gui.frame - careful"), out);
    }

    @Test
    void aDisabledLevelBuildsNothing() {
        Logging.configure(config(Mode.DEV, Level.WARN, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        Log log = Log.of("quiet");
        boolean[] built = {false};
        log.debug(() -> {
            built[0] = true;
            return "expensive";
        });
        log.trace(() -> {
            built[0] = true;
            return "expensive";
        });
        assertFalse(built[0], "a supplier for a disabled level must not run");
        assertFalse(log.isDebug());
        assertTrue(log.enabled(Level.WARN));
    }

    @Test
    void aPerLoggerLevelReachesEverySinkThatIsOnWhateverTheirThresholds(@TempDir Path dir) {
        Logging.configure(config(Mode.DEV, Level.WARN, Level.OFF, Map.of("gui.frame", Level.TRACE), dir,
                LogConfig.Format.TEXT));
        String out = console(() -> {
            Log.of("gui.frame.pacing").trace("every step");
            Log.of("vulkan").trace("not asked for");
        });
        assertTrue(out.contains("every step"), "named, so it is heard: " + out);
        assertFalse(out.contains("not asked for"), out);
    }

    @Test
    void theLevelCanBeChangedWhileRunning() {
        Logging.configure(config(Mode.DEV, Level.WARN, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        Log log = Log.of("live");
        assertFalse(log.isDebug());
        Logging.setLevel("live", Level.DEBUG);
        assertTrue(log.isDebug(), "a logger already held must see the change");
        assertTrue(console(() -> log.debug("now")).contains("now"));
        Logging.setLevel(null, Level.ERROR);
        assertFalse(Log.of("other").enabled(Level.WARN));
    }

    @Test
    void aThrowableLeftOverAfterThePlaceholdersIsTheException() {
        Logging.configure(config(Mode.DEV, Level.INFO, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        try (Logging.Capture capture = Logging.capture()) {
            Log.of("ex").error("failed {}", "thing", new IllegalStateException("boom"));
            Log.of("ex").warn("also failed", new RuntimeException("bang"));
            List<Logging.Entry> records = capture.records();
            assertEquals("failed thing", records.get(0).message());
            assertEquals("boom", records.get(0).thrown().getMessage());
            assertEquals("bang", records.get(1).thrown().getMessage());
        }
    }

    @Test
    void anArgumentWhoseToStringThrowsCannotTakeTheLoggerDown() {
        Logging.configure(config(Mode.DEV, Level.INFO, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        Object bad = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("no");
            }
        };
        try (Logging.Capture capture = Logging.capture()) {
            console(() -> Log.of("ts").info("value {}", bad));
            assertTrue(capture.records().get(0).message().contains("toString() threw"));
        }
    }

    @Test
    void warnOnceSaysItOnceForAKey() {
        Logging.configure(config(Mode.DEV, Level.INFO, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        Log log = Log.of("once");
        try (Logging.Capture capture = Logging.capture()) {
            console(() -> {
                assertTrue(log.warnOnce("site", "dropped"));
                assertFalse(log.warnOnce("site", "dropped"));
                assertTrue(log.warnOnce("other", "dropped again"));
            });
            assertEquals(2, capture.records().size());
        }
    }

    @Test
    void captureSeesWhatPassesALoggerEvenWhenNoSinkPrintsIt() {
        Logging.configure(config(Mode.TEST, Level.WARN, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        try (Logging.Capture capture = Logging.capture()) {
            String out = console(() -> Log.of("c").warn("seen"));
            assertTrue(capture.has(Level.WARN, "seen"));
            assertTrue(out.contains("seen"));
        }
        // closed: no longer collecting
        Logging.Capture late = Logging.capture();
        late.close();
        console(() -> Log.of("c").warn("after"));
        assertFalse(late.has(Level.WARN, "after"));
    }

    @Test
    void theFileGetsWhatItsThresholdAdmitsInFullTimestampedForm(@TempDir Path dir) throws IOException {
        Logging.configure(config(Mode.AUTOMATION, Level.OFF, Level.DEBUG, Map.of(), dir, LogConfig.Format.TEXT));
        Log.of("app.save").debug("wrote {} bytes", 42);
        Log.of("app.save").trace("not this one");
        // The banner is the first line of every configured file; what was asked for follows it.
        String text = Files.readAllLines(dir.resolve("t.log")).get(1);
        assertTrue(text.matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z DEBUG \\[.*\\] app.save - wrote 42 bytes"),
                text);
    }

    @Test
    void anExceptionIsWrittenWithItsStackIntoTheFile(@TempDir Path dir) throws IOException {
        Logging.configure(config(Mode.AUTOMATION, Level.OFF, Level.INFO, Map.of(), dir, LogConfig.Format.TEXT));
        Log.of("boom").error("it failed", new IllegalStateException("kaput"));
        String text = Files.readString(dir.resolve("t.log"));
        assertTrue(text.contains("java.lang.IllegalStateException: kaput") && text.contains("\tat "), text);
    }

    @Test
    void jsonIsOneObjectPerLineWithEverythingEscaped(@TempDir Path dir) throws IOException {
        Logging.configure(config(Mode.AUTOMATION, Level.OFF, Level.INFO, Map.of(), dir, LogConfig.Format.JSON));
        Log.of("j").info("say \"hi\"\nthere");
        Log.of("j").error("bad", new RuntimeException("x"));
        List<String> lines = Files.readAllLines(dir.resolve("t.log")).subList(1, 3);   // after the banner
        assertTrue(lines.get(0).contains("\"level\":\"INFO\"") && lines.get(0).contains("\"logger\":\"j\""), lines.get(0));
        assertTrue(lines.get(0).contains("\"msg\":\"say \\\"hi\\\"\\nthere\""), lines.get(0));
        assertTrue(lines.get(1).contains("\"exception\":\"java.lang.RuntimeException: x"), lines.get(1));
        assertTrue(lines.stream().allMatch(l -> l.startsWith("{") && l.endsWith("}")), lines.toString());
    }

    @Test
    void theBannerSaysWhatWasDecidedAndWhatWasIgnored(@TempDir Path dir) throws IOException {
        LogConfig c = new LogConfig("banner", Mode.AUTOMATION, Level.OFF, Level.INFO, Map.of(), dir,
                LogConfig.Format.TEXT, 1024, 1, null, List.of("log.level=loud is not a level"));
        String out = console(() -> {
            // configure(LogConfig) announces, like configure(String, Mode)
            Logging.configure(c);
        });
        String file = Files.readString(dir.resolve("banner.log"));
        assertTrue(file.contains("starting banner: mode=AUTOMATION"), file);
        assertTrue(file.contains("log setting ignored: log.level=loud is not a level"), file);
        assertNotNull(out);
    }

    @Test
    void nothingIsWrittenToStandardOutput() {
        Logging.configure(config(Mode.DEV, Level.TRACE, Level.OFF, Map.of(), null, LogConfig.Format.TEXT));
        PrintStream real = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bytes, true, StandardCharsets.UTF_8));
        try {
            console(() -> Log.of("o").info("hello"));
        } finally {
            System.setOut(real);
        }
        assertEquals("", bytes.toString(StandardCharsets.UTF_8),
                "standard output is what a program produces, and ottermate reads the port from it");
    }
}
