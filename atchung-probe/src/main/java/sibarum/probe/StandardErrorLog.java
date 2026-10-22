package sibarum.probe;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * What {@code System.err} writes into once {@link Logging#captureStandardError} has replaced it: the text becomes
 * records of the logger {@code stderr}, at {@code WARN}, so each is in the file as soon as it is delivered.
 *
 * <p><b>A record per flush, not per line.</b> {@code printStackTrace} writes a trace a line at a time and
 * {@code Fatal.HALT} flushes once after it, so delivering on {@link #flush} keeps a trace in one record. Text
 * written without a flush is not left waiting: the log's flusher {@link #drain drains} whole lines once a second,
 * and the shutdown hook drains the rest.
 */
final class StandardErrorLog extends OutputStream {

    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();

    @Override
    public synchronized void write(int b) {
        pending.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        pending.write(b, off, len);
    }

    @Override
    public void flush() {
        drain(true);
    }

    /**
     * Deliver what is waiting as one record.
     *
     * @param partial whether a last line with no newline yet goes too; otherwise it waits for the rest of itself
     */
    void drain(boolean partial) {
        String text;
        synchronized (this) {
            byte[] bytes = pending.toByteArray();
            int end = bytes.length;
            if (!partial) {
                while (end > 0 && bytes[end - 1] != '\n') {
                    end--;
                }
            }
            if (end == 0) {
                return;
            }
            text = new String(bytes, 0, end, StandardCharsets.UTF_8);
            pending.reset();
            pending.write(bytes, end, bytes.length - end);
        }
        text = text.stripTrailing();
        if (!text.isEmpty()) {
            // Outside the lock, so a thread writing to System.err does not wait on the disk.
            Logging.emit("stderr", Level.WARN, text, null);
        }
    }
}
