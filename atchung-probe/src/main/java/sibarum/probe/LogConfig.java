package sibarum.probe;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Everything logging has decided to do, worked out once from the situation and the settings.
 *
 * <p>{@link #resolve} is a pure function of an {@link Env} — the settings, the working directory, the home
 * directory and three facts about the process — so every default in this file is asserted by a test that never
 * touches a real property, a real file or a real JVM flag. Nothing here opens a file or starts a thread.
 *
 * <h2>The settings</h2>
 *
 * Each is a system property, or the same name in upper case with dots as underscores as an environment variable
 * ({@code log.level} / {@code LOG_LEVEL}); the property wins.
 *
 * <pre>
 * log.mode=automation         force a mode (test, automation, dev, packaged) instead of detecting one
 * log.level=debug             both sinks; the two below are more specific and win over it
 * log.console=warn            console threshold, or off
 * log.file=trace              file threshold, or off
 * log.level.gui.frame=trace   one logger and everything beneath it; see below
 * log.dir=/var/log/app        where the file goes
 * log.format=json             text (the default) or json, one object per line
 * log.rotate.size=10          megabytes before the file rolls over (default 5)
 * log.rotate.keep=3           rolled files kept beside the live one (default 5)
 * log.app=editor              the name the file and the banner use
 * </pre>
 *
 * <h2>What a mode implies</h2>
 *
 * <pre>
 *                console   file    probe
 * TEST           warn      off     off
 * AUTOMATION     debug     trace   frame,input,layout,app, with a correlation trace beside the log
 * DEV            info      debug   off
 * PACKAGED       warn      info    off
 * </pre>
 *
 * <h2>A per-logger level is an instruction about that logger</h2>
 *
 * A sink's threshold filters the loggers that have nothing to say about themselves. A logger with its own
 * {@code log.level.<name>} is being asked for by name, so what it emits reaches every sink that is on, whatever
 * that sink's threshold — otherwise {@code -Dlog.level.gui.frame=trace} would change nothing in a shipped
 * configuration, which is exactly when somebody types it.
 *
 * <h2>Where the file goes</h2>
 *
 * <ol>
 *   <li>{@code log.dir}, if set;</li>
 *   <li>{@code <app>.home/logs}, if the {@code <app>.home} property is set — the same redirect {@code AppHome}
 *       honours, so a test rig that moves an application's home moves its logs with it;</li>
 *   <li>in {@link Mode#DEV} and {@link Mode#AUTOMATION}, {@code ./target/logs} when the working directory is a
 *       Maven project, so the log sits beside the build and {@code mvn clean} removes it;</li>
 *   <li>otherwise {@code $HOME/.<app>/logs}, the per-user directory {@code AppHome} names.</li>
 * </ol>
 *
 * One layout on every platform, for the reason {@code AppHome} gives: a convention that can be stated in one line
 * is one a user can find without documentation.
 *
 * @param app         the application's name, used for the file and the banner
 * @param mode        what kind of run this is
 * @param console     the console threshold
 * @param file        the file threshold
 * @param overrides   per-logger levels, keyed by logger name; the longest key that is a dotted prefix wins
 * @param dir         the log directory, or {@code null} when the file is off
 * @param format      how a line is written
 * @param rotateBytes size at which the file rolls over
 * @param rotateKeep  rolled files kept
 * @param probeLanes  the probe lanes this mode turns on by default, or {@code null} to leave the probe off
 * @param problems    settings that were present and not understood, for the banner to say so
 */
public record LogConfig(String app, Mode mode, Level console, Level file, Map<String, Level> overrides,
                        Path dir, Format format, long rotateBytes, int rotateKeep, String probeLanes,
                        List<String> problems) {

    /** How a line is written. */
    public enum Format { TEXT, JSON }

    /** The default size a file rolls at: large enough to hold a long session, small enough to open. */
    public static final long DEFAULT_ROTATE_MB = 5;

    public static final int DEFAULT_ROTATE_KEEP = 5;

    public LogConfig {
        overrides = Map.copyOf(overrides);
        problems = List.copyOf(problems);
    }

    /**
     * What resolution needs to know about the world.
     *
     * @param settingNames every setting name that exists, so {@code log.level.<logger>} can be found by prefix
     * @param settings    a setting by its property name, or {@code null}; the production form reads the
     *                    property and then the environment
     * @param cwd         the working directory
     * @param home        the user's home directory
     * @param nativeImage whether this is a GraalVM native image
     * @param testRun     whether a test runner launched this JVM
     * @param isFile      whether a path is an existing regular file
     */
    public record Env(Function<String, String> settings, java.util.function.Supplier<java.util.Collection<String>> settingNames,
                      Path cwd, Path home, boolean nativeImage,
                      boolean testRun, Predicate<Path> isFile) {

        /** The real process: its properties, its environment, its directory. */
        public static Env system() {
            String classPath = System.getProperty("java.class.path", "");
            boolean test = System.getProperty("surefire.real.class.path") != null
                    || System.getProperty("surefire.test.class.path") != null
                    || classPath.contains("surefirebooter");
            return new Env(LogConfig::systemSetting, () -> System.getProperties().stringPropertyNames(),
                    Path.of("").toAbsolutePath(),
                    Path.of(System.getProperty("user.home", ".")),
                    System.getProperty("org.graalvm.nativeimage.imagecode") != null,
                    test,
                    Files::isRegularFile);
        }

        /**
         * This world with {@code launch} settings laid over it: a value here wins over the property and the
         * environment, because a flag on this launch is more specific than either. What a host hands over when
         * it knows something the properties do not — {@code --automation=0} is a command-line flag, not a
         * system property, and the mode has to be known before anything logs.
         */
        public Env with(Map<String, String> launch) {
            Function<String, String> base = settings;
            java.util.function.Supplier<java.util.Collection<String>> baseNames = settingNames;
            return new Env(key -> {
                String v = launch.get(key);
                return v != null && !v.isBlank() ? v.trim() : base.apply(key);
            }, () -> {
                java.util.Set<String> names = new java.util.HashSet<>(baseNames.get());
                names.addAll(launch.keySet());
                return names;
            }, cwd, home, nativeImage, testRun, isFile);
        }
    }

    /** A setting from the system property, then the environment ({@code log.level} becomes {@code LOG_LEVEL}). */
    static String systemSetting(String key) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            v = System.getenv(key.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT));
        }
        return v == null || v.isBlank() ? null : v.trim();
    }

    /**
     * Work out the configuration.
     *
     * @param app      the application's name, or {@code null} to use {@code log.app} and then {@code "vexelray"}
     * @param explicit a mode the caller knows — the framework knows it is being driven before any property says
     *                 so — or {@code null} to detect one
     * @param env      the world
     */
    public static LogConfig resolve(String app, Mode explicit, Env env) {
        List<String> problems = new ArrayList<>();
        Function<String, String> get = env.settings();

        String name = slug(first(app, get.apply("log.app"), "vexelray"));
        Mode mode = mode(explicit, get, env, problems);

        Level defaultConsole;
        Level defaultFile;
        switch (mode) {
            case TEST -> { defaultConsole = Level.WARN; defaultFile = Level.OFF; }
            case AUTOMATION -> { defaultConsole = Level.DEBUG; defaultFile = Level.TRACE; }
            case DEV -> { defaultConsole = Level.INFO; defaultFile = Level.DEBUG; }
            default -> { defaultConsole = Level.WARN; defaultFile = Level.INFO; }
        }
        Level general = level(get, "log.level", null, problems);
        Level console = level(get, "log.console", general != null ? general : defaultConsole, problems);
        Level file = level(get, "log.file", general != null ? general : defaultFile, problems);

        Map<String, Level> overrides = new TreeMap<>();
        for (String key : overrideKeys(env)) {
            Level l = level(get, key, null, problems);
            if (l != null) {
                overrides.put(key.substring("log.level.".length()), l);
            }
        }

        LogConfig.Format format = LogConfig.Format.TEXT;
        String f = get.apply("log.format");
        if (f != null) {
            if (f.equalsIgnoreCase("json")) {
                format = LogConfig.Format.JSON;
            } else if (!f.equalsIgnoreCase("text")) {
                problems.add("log.format=" + f + " is not text or json");
            }
        }

        long rotateMb = number(get, "log.rotate.size", DEFAULT_ROTATE_MB, problems);
        int keep = (int) number(get, "log.rotate.keep", DEFAULT_ROTATE_KEEP, problems);

        boolean fileOn = file != Level.OFF || anyOverride(overrides);
        Path dir = fileOn ? directory(name, mode, get, env) : null;
        String lanes = mode == Mode.AUTOMATION ? "frame,input,layout,app" : null;

        return new LogConfig(name, mode, console, file, overrides, dir, format,
                Math.max(1, rotateMb) * 1024L * 1024L, Math.max(0, keep), lanes, problems);
    }

    /** Whether the file is worth opening at all. */
    public boolean fileEnabled() {
        return dir != null;
    }

    /** The live log file, or {@code null} when the file is off. */
    public Path logFile() {
        return dir == null ? null : dir.resolve(app + ".log");
    }

    /** Where the probe's correlation trace goes when this mode turns the probe on by default. */
    public Path probeFile() {
        return dir == null ? null : dir.resolve(app + "-probe.csv");
    }

    /**
     * The level a logger is held to by name: the longest override that is {@code name} or a dotted prefix of it,
     * or {@code null} if nothing names it.
     */
    public Level overrideFor(String name) {
        Level best = null;
        int bestLength = -1;
        for (Map.Entry<String, Level> e : overrides.entrySet()) {
            String key = e.getKey();
            boolean applies = name.equals(key) || name.startsWith(key + ".");
            if (applies && key.length() > bestLength) {
                best = e.getValue();
                bestLength = key.length();
            }
        }
        return best;
    }

    /**
     * The least severe level any sink wants, which is what a logger without an override of its own is held to:
     * below it, nothing would be written anywhere, so building the record is wasted work.
     */
    public Level floor() {
        Level lowest = Level.OFF;
        for (Level l : new Level[]{console, file}) {
            if (l != Level.OFF && l.ordinal() < lowest.ordinal()) {
                lowest = l;
            }
        }
        return lowest;
    }

    // --- resolution ---------------------------------------------------------------------------------------

    private static Mode mode(Mode explicit, Function<String, String> get, Env env, List<String> problems) {
        if (explicit != null) {
            return explicit;
        }
        String named = get.apply("log.mode");
        if (named != null) {
            try {
                return Mode.valueOf(named.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                problems.add("log.mode=" + named + " is not one of test, automation, dev, packaged");
            }
        }
        String automation = get.apply("automation");
        if (automation != null && !automation.equalsIgnoreCase("off") && !automation.equalsIgnoreCase("false")) {
            return Mode.AUTOMATION;
        }
        if (env.testRun()) {
            return Mode.TEST;
        }
        return env.nativeImage() ? Mode.PACKAGED : Mode.DEV;
    }

    private static Path directory(String app, Mode mode, Function<String, String> get, Env env) {
        String dir = get.apply("log.dir");
        if (dir != null) {
            return Path.of(dir).toAbsolutePath().normalize();
        }
        String home = get.apply(app + ".home");
        if (home != null) {
            return Path.of(home).toAbsolutePath().normalize().resolve("logs");
        }
        if ((mode == Mode.DEV || mode == Mode.AUTOMATION) && env.isFile().test(env.cwd().resolve("pom.xml"))) {
            return env.cwd().resolve("target").resolve("logs");
        }
        return env.home().resolve("." + app).resolve("logs");
    }

    private static Level level(Function<String, String> get, String key, Level fallback, List<String> problems) {
        String v = get.apply(key);
        if (v == null) {
            return fallback;
        }
        Level l = Level.parse(v);
        if (l == null) {
            problems.add(key + "=" + v + " is not a level (trace, debug, info, warn, error, off)");
            return fallback;
        }
        return l;
    }

    private static long number(Function<String, String> get, String key, long fallback, List<String> problems) {
        String v = get.apply(key);
        if (v == null) {
            return fallback;
        }
        try {
            long n = Long.parseLong(v);
            if (n >= 0) {
                return n;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        problems.add(key + "=" + v + " is not a non-negative number");
        return fallback;
    }

    private static boolean anyOverride(Map<String, Level> overrides) {
        return overrides.values().stream().anyMatch(l -> l != Level.OFF);
    }

    /**
     * The {@code log.level.<logger>} settings that exist. Properties can be enumerated; an environment cannot be
     * asked by prefix through a lookup function, so only properties name loggers — which is also the only place
     * a dotted logger name survives, since an environment variable cannot tell a dot from an underscore.
     */
    private static List<String> overrideKeys(Env env) {
        List<String> keys = new ArrayList<>();
        for (String key : env.settingNames().get()) {
            if (key.startsWith("log.level.") && key.length() > "log.level.".length()) {
                keys.add(key);
            }
        }
        return keys;
    }

    private static String first(String... candidates) {
        for (String c : candidates) {
            if (c != null && !c.isBlank()) {
                return c.trim();
            }
        }
        return "vexelray";
    }

    /** A name fit for a file and a dot-directory: lower case, letters, digits and dashes. */
    static String slug(String name) {
        String s = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("^-+|-+$", "");
        return s.isEmpty() ? "vexelray" : s;
    }
}
