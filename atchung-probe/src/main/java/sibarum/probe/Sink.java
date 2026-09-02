package sibarum.probe;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Where the probe's output goes: a file if one was named, otherwise the process's own stdout.
 *
 * <p>Writes are serialised on this object. That is a lock on a path taken once per traced event and once per
 * report, never per span — spans accumulate into a {@link Tally} lock-free and are only rendered when someone
 * asks. Profiling that contends with itself measures the contention, so the boundary between "record" and
 * "emit" is the boundary between lock-free and locked, and it is the whole reason those are two operations.
 *
 * <p>A file sink is opened once and flushed after every write. Flushing costs a syscall per line, and it is
 * not negotiable: the run this facility is for is the one that ends in a hang, a kill, or a driver reset, and
 * a buffered tail lost at exactly that moment is the tail that mattered.
 */
final class Sink {

    private final Writer out;
    private final Path path;
    private final boolean ownsStream;

    private Sink(Writer out, Path path, boolean ownsStream) {
        this.out = out;
        this.path = path;
        this.ownsStream = ownsStream;
    }

    /** Standard output, unowned — never closed, because it is not ours to close. */
    static Sink stdout() {
        return new Sink(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), null, false);
    }

    /**
     * A file at {@code path}, truncated. Falls back to {@link #stdout()} with one line of explanation if the
     * file cannot be opened: a profiling run that dies because its log directory does not exist has wasted the
     * reproduction, and the reproduction is the expensive part.
     */
    static Sink file(Path path) {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Writer w = new BufferedWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));
            return new Sink(w, path, true);
        } catch (IOException e) {
            Sink fallback = stdout();
            fallback.line("probe: cannot write " + path + " (" + e.getMessage() + "); using stdout");
            return fallback;
        }
    }

    /** The file being written, or {@code null} for stdout. Reported at startup so the run says where it went. */
    Path path() {
        return path;
    }

    /** One line, flushed. */
    synchronized void line(String s) {
        try {
            out.write(s);
            out.write(System.lineSeparator());
            out.flush();
        } catch (IOException e) {
            // Nowhere left to report to; a probe that throws out of a call site would turn a performance
            // question into a crash, which is strictly worse than losing the line.
        }
    }

    /** A block of already-formatted lines, written and flushed as one. */
    synchronized void block(String s) {
        try {
            out.write(s);
            out.flush();
        } catch (IOException e) {
            // As above.
        }
    }

    synchronized void close() {
        try {
            out.flush();
            if (ownsStream) {
                out.close();
            }
        } catch (IOException e) {
            // As above.
        }
    }
}
