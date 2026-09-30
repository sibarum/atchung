package sibarum.probe;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The defaults, asserted as a table: what each situation does without being told, which is the whole claim of
 * a context-aware model. Nothing here touches a real property, file or JVM flag.
 */
class LogConfigTest {

    private static final Path CWD = Path.of("/work/app").toAbsolutePath();
    private static final Path HOME = Path.of("/home/me").toAbsolutePath();

    private static LogConfig.Env env(Map<String, String> settings, boolean nativeImage, boolean test, boolean pom) {
        return new LogConfig.Env(settings::get, settings::keySet, CWD, HOME, nativeImage, test,
                p -> pom && p.equals(CWD.resolve("pom.xml")));
    }

    private static LogConfig resolve(Map<String, String> settings, boolean nativeImage, boolean test, boolean pom) {
        return LogConfig.resolve("editor", null, env(settings, nativeImage, test, pom));
    }

    @Test
    void aTestRunIsQuietAndWritesNoFile() {
        LogConfig c = resolve(Map.of(), false, true, true);
        assertEquals(Mode.TEST, c.mode());
        assertEquals(Level.WARN, c.console());
        assertEquals(Level.OFF, c.file());
        assertFalse(c.fileEnabled());
        assertNull(c.probeLanes());
    }

    @Test
    void aDevelopmentRunSaysWhatItIsDoingAndKeepsADetailedFile() {
        LogConfig c = resolve(Map.of(), false, false, true);
        assertEquals(Mode.DEV, c.mode());
        assertEquals(Level.INFO, c.console());
        assertEquals(Level.DEBUG, c.file());
        assertNull(c.probeLanes(), "profiling is not free, so it is never on unasked in an ordinary run");
    }

    @Test
    void aPackagedApplicationIsQuietOnTheConsoleAndKeepsAModestFile() {
        LogConfig c = resolve(Map.of(), true, false, false);
        assertEquals(Mode.PACKAGED, c.mode());
        assertEquals(Level.WARN, c.console());
        assertEquals(Level.INFO, c.file());
    }

    @Test
    void aDrivenRunIsTheLoudestAndTurnsTheProbeOn() {
        LogConfig c = resolve(Map.of("automation", "0"), false, false, true);
        assertEquals(Mode.AUTOMATION, c.mode());
        assertEquals(Level.DEBUG, c.console());
        assertEquals(Level.TRACE, c.file());
        assertEquals("frame,input,layout,app", c.probeLanes());
        assertEquals(c.dir().resolve("editor-probe.csv"), c.probeFile());
    }

    @Test
    void automationBeatsPackagedBecauseSomebodyIsReadingAlong() {
        assertEquals(Mode.AUTOMATION, resolve(Map.of("automation", "on"), true, false, false).mode());
    }

    @Test
    void automationSetToOffIsNotAutomation() {
        assertEquals(Mode.DEV, resolve(Map.of("automation", "off"), false, false, true).mode());
        assertEquals(Mode.DEV, resolve(Map.of("automation", "false"), false, false, true).mode());
    }

    @Test
    void theCallerMayKnowTheModeBeforeAnyPropertySaysSo() {
        // The framework sees --automation on the command line, which is not a system property.
        LogConfig c = LogConfig.resolve("editor", Mode.AUTOMATION, env(Map.of(), false, false, true));
        assertEquals(Mode.AUTOMATION, c.mode());
    }

    @Test
    void theModeCanBeForcedAndABadOneIsReportedNotObeyed() {
        assertEquals(Mode.PACKAGED, resolve(Map.of("log.mode", "packaged"), false, false, true).mode());
        LogConfig bad = resolve(Map.of("log.mode", "loud"), false, false, true);
        assertEquals(Mode.DEV, bad.mode());
        assertTrue(bad.problems().stream().anyMatch(p -> p.contains("log.mode=loud")), bad.problems().toString());
    }

    @Test
    void theGeneralLevelSetsBothSinksAndTheSpecificOnesWinOverIt() {
        LogConfig general = resolve(Map.of("log.level", "debug"), true, false, false);
        assertEquals(Level.DEBUG, general.console());
        assertEquals(Level.DEBUG, general.file());

        LogConfig specific = resolve(Map.of("log.level", "debug", "log.console", "error", "log.file", "off"),
                true, false, false);
        assertEquals(Level.ERROR, specific.console());
        assertEquals(Level.OFF, specific.file());
        assertFalse(specific.fileEnabled());
    }

    @Test
    void aLevelThatIsNotOneIsReportedAndTheDefaultStands() {
        LogConfig c = resolve(Map.of("log.console", "shouty"), false, false, true);
        assertEquals(Level.INFO, c.console());
        assertTrue(c.problems().get(0).contains("log.console=shouty"));
    }

    @Test
    void theFileGoesBesideTheBuildInAProject() {
        assertEquals(CWD.resolve("target").resolve("logs"), resolve(Map.of(), false, false, true).dir());
    }

    @Test
    void theFileGoesToTheUsersDotDirectoryOutsideAProjectAndWhenPackaged() {
        Path expected = HOME.resolve(".editor").resolve("logs");
        assertEquals(expected, resolve(Map.of(), false, false, false).dir());
        assertEquals(expected, resolve(Map.of(), true, false, true).dir(), "a packaged app never logs into a checkout");
    }

    @Test
    void theApplicationsHomeRedirectMovesItsLogsWithIt() {
        Path home = Path.of("/rig/home").toAbsolutePath();
        LogConfig c = resolve(Map.of("editor.home", home.toString()), false, false, true);
        assertEquals(home.resolve("logs"), c.dir(), "the same redirect AppHome honours");
    }

    @Test
    void anExplicitDirectoryWinsOverEverything() {
        Path dir = Path.of("/var/log/editor").toAbsolutePath();
        LogConfig c = resolve(Map.of("log.dir", dir.toString(), "editor.home", "/elsewhere"), false, false, true);
        assertEquals(dir, c.dir());
        assertEquals(dir.resolve("editor.log"), c.logFile());
    }

    @Test
    void rotationDefaultsAndOverrides() {
        LogConfig d = resolve(Map.of(), false, false, true);
        assertEquals(5L * 1024 * 1024, d.rotateBytes());
        assertEquals(5, d.rotateKeep());
        LogConfig o = resolve(Map.of("log.rotate.size", "2", "log.rotate.keep", "0"), false, false, true);
        assertEquals(2L * 1024 * 1024, o.rotateBytes());
        assertEquals(0, o.rotateKeep());
    }

    @Test
    void theFormatIsTextUnlessJsonIsAskedFor() {
        assertEquals(LogConfig.Format.TEXT, resolve(Map.of(), false, false, true).format());
        assertEquals(LogConfig.Format.JSON, resolve(Map.of("log.format", "JSON"), false, false, true).format());
        assertTrue(resolve(Map.of("log.format", "xml"), false, false, true).problems().get(0).contains("xml"));
    }

    @Test
    void theApplicationNameBecomesAFileSafeSlug() {
        LogConfig c = LogConfig.resolve("My Editor 2!", null, env(Map.of(), false, false, true));
        assertEquals("my-editor-2", c.app());
        assertEquals("vexelray", LogConfig.resolve(null, null, env(Map.of(), false, false, true)).app());
        assertEquals("fromprop", LogConfig.resolve(null, null, env(Map.of("log.app", "fromprop"), false, false, true)).app());
    }

    @Test
    void theLongestOverrideThatIsAPrefixWins() {
        Map<String, String> s = new HashMap<>();
        s.put("log.level.gui", "debug");
        s.put("log.level.gui.frame", "trace");
        s.put("log.level.guide", "error");
        LogConfig c = resolve(s, true, false, false);
        assertEquals(Level.TRACE, c.overrideFor("gui.frame"));
        assertEquals(Level.TRACE, c.overrideFor("gui.frame.pacing"));
        assertEquals(Level.DEBUG, c.overrideFor("gui.layout"));
        assertEquals(Level.ERROR, c.overrideFor("guide"), "a prefix is a dotted prefix, not a string prefix");
        assertNull(c.overrideFor("vulkan"));
    }

    @Test
    void anOverrideKeepsTheFileOpenEvenWhenTheFileIsOff() {
        LogConfig c = resolve(Map.of("log.file", "off", "log.level.gui.frame", "trace"), true, false, false);
        assertTrue(c.fileEnabled(), "asking for a logger by name has to reach somewhere");
    }

    @Test
    void theFloorIsTheLowestLevelAnySinkWants() {
        assertEquals(Level.DEBUG, resolve(Map.of(), false, false, true).floor());
        assertEquals(Level.WARN, resolve(Map.of(), false, true, true).floor());
        assertEquals(Level.OFF, resolve(Map.of("log.console", "off", "log.file", "off"), false, false, true).floor());
    }
}
