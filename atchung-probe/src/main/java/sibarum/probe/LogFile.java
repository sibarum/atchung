package sibarum.probe;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * A log file that rolls over by size, is created only when something is written to it, and never throws.
 *
 * <p><b>Reading leaves no mark.</b> Like {@code AppHome}, nothing touches the disk until the first line: an
 * application launched and closed without logging anything at a level that reaches the file leaves no directory
 * behind. The file and its directory appear together, at the first write.
 *
 * <p><b>Rolling.</b> When the next line would take the live file past {@code maxBytes}, it becomes
 * {@code name.1}, what was {@code name.1} becomes {@code name.2}, and so on up to {@code keep}; the oldest is
 * deleted. {@code keep == 0} keeps no history: the file is simply started again. A line is never split across two
 * files, and a single line longer than the limit is written whole rather than refused.
 *
 * <p><b>It cannot fail the application.</b> A log that cannot be written — a read-only directory, a full disk, a
 * path that is a file — is reported once on standard error and the file is given up on; the records still reach
 * the console. An application that stopped because its log could not be written would have turned a diagnostic
 * into the fault.
 */
final class LogFile implements AutoCloseable {

    private final Path path;
    private final long maxBytes;
    private final int keep;

    private BufferedWriter out;
    private long size;
    private boolean dead;
    private boolean dirty;

    LogFile(Path path, long maxBytes, int keep) {
        this.path = path;
        this.maxBytes = maxBytes;
        this.keep = keep;
    }

    Path path() {
        return path;
    }

    /**
     * Append {@code text}, which is a whole line or several, already newline-terminated.
     *
     * @param flushNow push it to the disk before returning, for the records that matter if the process dies next
     */
    synchronized void write(String text, boolean flushNow) {
        if (dead) {
            return;
        }
        try {
            if (out == null) {
                open();
            }
            long bytes = text.getBytes(StandardCharsets.UTF_8).length;
            if (size > 0 && size + bytes > maxBytes) {
                roll();
            }
            out.write(text);
            size += bytes;
            dirty = true;
            if (flushNow) {
                out.flush();
                dirty = false;
            }
        } catch (IOException | RuntimeException e) {
            giveUp(e);
        }
    }

    /** Push anything buffered to the disk. Cheap when nothing is. */
    synchronized void flush() {
        if (out == null || !dirty || dead) {
            return;
        }
        try {
            out.flush();
            dirty = false;
        } catch (IOException e) {
            giveUp(e);
        }
    }

    @Override
    public synchronized void close() {
        if (out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                // closing a file that could not be written is not worth a second report
            }
            out = null;
        }
    }

    private void open() throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        size = Files.exists(path) ? Files.size(path) : 0L;
        out = new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                StandardCharsets.UTF_8));
    }

    private void roll() throws IOException {
        out.flush();
        out.close();
        out = null;
        if (keep == 0) {
            Files.deleteIfExists(path);
        } else {
            Files.deleteIfExists(numbered(keep));
            for (int i = keep - 1; i >= 1; i--) {
                Path from = numbered(i);
                if (Files.exists(from)) {
                    Files.move(from, numbered(i + 1), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Files.move(path, numbered(1), StandardCopyOption.REPLACE_EXISTING);
        }
        size = 0;
        open();
    }

    private Path numbered(int n) {
        return path.resolveSibling(path.getFileName() + "." + n);
    }

    private void giveUp(Exception e) {
        dead = true;
        out = null;
        System.err.println("vexelray: log file " + path + " cannot be written (" + e + "); logging to the console only");
    }
}
