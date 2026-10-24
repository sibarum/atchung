package sibarum.probe;

/**
 * In-process code that wants the probe's rows as they happen, rather than from the file afterwards.
 *
 * <p>Attach one with {@link Probe#attach} and it is handed every row the output gets — the same rows, in the same
 * order, under the same {@code seq} — for as long as it stays attached. A row is the correlation log's row
 * ({@code seq,t_mono_ns,t_wall,thread,lane,kind,cause,detail}, see {@code docs/probe.md}) and means the same thing,
 * whatever format the file itself is in.
 *
 * <h2>The fields, not an object</h2>
 *
 * A row arrives as its fields, as arguments. There is no row object, so handing it over allocates nothing, and
 * nothing is shared between calls that a sink could mistakenly keep. A sink that wants to keep a row copies the
 * fields into storage of its own — a ring of parallel arrays allocated once is the intended shape. The strings are
 * the ones the probe already built for the line; keeping a reference to one is free and safe.
 *
 * <p>They are the <em>values</em>, not the CSV encoding of them: {@code detail} is exactly what the call site
 * passed (never {@code null}; an absent detail is {@code ""}), unquoted, and with any line breaks it had. Quoting
 * and flattening are the file's concern.
 *
 * <h2>What a sink may do in here</h2>
 *
 * {@link #row} is called on the emitting thread, inside the one lock that serialises the output, which is what
 * makes the order a sink sees the order the file has. So it must be short, it must not block, and it must not
 * call back into {@link Probe} — every other thread that emits a row is waiting on it. Copy and return.
 *
 * <p>A sink that throws costs that row and only that row: it is dropped for that sink, counted in
 * {@link Probe#liveSinkFailures()}, and the first failure of each sink is reported once on stderr. The sink stays
 * attached, and the dropped row shows up to it the way any loss does — as a gap in {@code seq}.
 */
@FunctionalInterface
public interface LiveSink {

    /**
     * One row.
     *
     * @param seq     the row's number, from 1, gapless across the whole process — so a gap a sink sees is a row
     *                it was not given (it was attached late, or it threw), never a row that did not happen
     * @param tMonoNs nanoseconds since the probe started, monotonic; ascending in {@code seq} order
     * @param tWallNs the wall clock, as nanoseconds since the epoch — the file's {@code t_wall}, unformatted
     * @param thread  the emitting thread's name
     * @param lane    the lane; {@link Lane#key()} is the file's {@code lane} column
     * @param kind    what happened — the span, counter or mark name
     * @param detail  the producer's own detail, or {@code ""}
     */
    void row(long seq, long tMonoNs, long tWallNs, String thread, Lane lane, String kind, String detail);

    /**
     * One row, with the cause it was written in ({@link Cause#NONE} when there was none). This is the one the probe
     * calls. It hands the row to {@link #row(long, long, long, String, Lane, String, String)} without its cause, so a
     * sink written before causes existed goes on working; a sink that keeps causes overrides this as well.
     *
     * @param cause the cause, as {@link Cause#current()} gave it on the emitting thread
     */
    default void row(long seq, long tMonoNs, long tWallNs, String thread, Lane lane, String kind, String detail,
                     long cause) {
        row(seq, tMonoNs, tWallNs, thread, lane, kind, detail);
    }
}
