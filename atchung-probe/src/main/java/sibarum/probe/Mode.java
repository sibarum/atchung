package sibarum.probe;

/**
 * What kind of run this is, which is what decides how much it says and where.
 *
 * <p>One process, one mode, worked out once at startup by {@link LogConfig#resolve}. The idea is that nobody
 * should have to set five properties to get sensible output: the situation already tells us most of what they
 * would have said. A person developing wants to see what is happening and to keep a detailed file for afterwards;
 * a shipped application wants to be quiet on the console and keep a modest file for support; a test wants to be
 * silent unless something is wrong; and <b>a run being driven through the automation socket has somebody or
 * something reading along</b>, so it says everything, because the point of driving it is to find out what it did.
 *
 * <p>Every default a mode implies is a default and nothing more: any of them is overridden by its own property
 * (see {@link LogConfig}), and the mode itself by {@code -Dlog.mode=}.
 */
public enum Mode {

    /**
     * Under a test runner. Quiet on the console and no file: a green build prints nothing, a red one prints what
     * went wrong. Records are still delivered to {@link Logging#capture}, so a test can assert that something was
     * logged without the suite being noisy.
     */
    TEST,

    /**
     * The automation socket is on — an agent, a script or {@code ottermate} is driving this run. The loudest mode:
     * {@code DEBUG} on the console, {@code TRACE} in the file, and the {@link Probe} switched on for the frame,
     * input, layout and app lanes with its correlation trace beside the log, so a run can be read back in order
     * afterwards without having to have asked for it in advance.
     */
    AUTOMATION,

    /**
     * Run from a checkout or an IDE: a JVM that is not a native image and has nobody driving it. {@code INFO} on
     * the console, {@code DEBUG} in a file that lives in the project's {@code target/} so it is found beside
     * the build and removed by {@code mvn clean}.
     */
    DEV,

    /**
     * A native image: what a user runs. {@code WARN} on the console, which is usually nowhere anyone looks, and
     * {@code INFO} in a per-user file, so that "it did something odd yesterday" has a record to answer it.
     */
    PACKAGED
}
