package sibarum.probe;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Reads back what {@code probe.format=csv} wrote: projections of the one file, and the gap hunt.
 *
 * <p>Beside the writer on purpose. Derived views are generated from the single log rather than written alongside
 * it, so they cannot drift from it — and a reader living in another repository would drift from the
 * <em>format</em> the first time it changed, which is the same failure one step further out.
 *
 * <h2>The gap hunt</h2>
 *
 * A run is read by sorting on the monotonic clock and looking for long stretches with no frame in them. That is
 * the highest-value question this format serves and the one thing {@code awk} cannot answer on its own, because
 * a gap is only a finding if the loop was <em>supposed</em> to be running:
 *
 * <ul>
 *   <li>{@code frame.present} is emitted every frame, so a stretch without one is a stretch with no frames.</li>
 *   <li>{@code loop.park} says the loop went to sleep and <b>how long it was allowed to sleep for</b>. A render-
 *       on-demand loop parks indefinitely when nobody is looking, so a thirty-second doze and a thirty-second
 *       hang are the same silence without it.</li>
 * </ul>
 *
 * So: a gap covered by the park that precedes it is expected, and a gap that is not is a stall — and the row
 * before it is the suspect. {@code --gaps} applies exactly that rule and prints the suspect.
 *
 * <pre>
 * csvview run.csv --gaps            gaps of 100ms or more, each judged against the preceding park
 * csvview run.csv --gaps 16         anything over one frame at 60Hz
 * csvview run.csv --lane input      only the input lane
 * csvview run.csv --kind wake       kinds containing "wake"
 * csvview run.csv --grep Gui@421    any row mentioning it
 * </pre>
 */
public final class CsvView {

    /** One row, parsed. {@code detail} keeps whatever the producer put there. */
    public record Row(long seq, long monoNanos, String wall, String thread, String lane, String kind,
                      String detail, String raw) {

        boolean isFrame() {
            return kind.equals("frame.present");
        }

        boolean isPark() {
            return kind.equals("loop.park");
        }

        /** How long this park was allowed to last, in nanos; {@link Long#MAX_VALUE} for "forever". */
        long parkBudgetNanos() {
            String d = detail.trim();
            if (d.equalsIgnoreCase("forever")) {
                return Long.MAX_VALUE;
            }
            try {
                return Long.parseLong(d.endsWith("ms") ? d.substring(0, d.length() - 2) : d) * 1_000_000L;
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
    }

    private CsvView() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("""
                    csvview <run.csv> [options]
                      --gaps [ms]     stretches with no frame in them, judged against the preceding park
                      --lane <names>  comma-separated lanes to keep
                      --kind <text>   kinds containing this
                      --thread <text> threads containing this
                      --grep <text>   rows mentioning this anywhere
                      --tail <n>      only the last n rows""");
            System.exit(2);
            return;
        }
        Path file = Path.of(args[0]);
        List<Row> rows = read(file);
        String lanes = null;
        String kind = null;
        String thread = null;
        String grep = null;
        int tail = 0;
        boolean gaps = false;
        long gapMillis = 100L;
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--gaps" -> {
                    gaps = true;
                    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        gapMillis = Long.parseLong(args[++i]);
                    }
                }
                case "--lane" -> lanes = args[++i];
                case "--kind" -> kind = args[++i];
                case "--thread" -> thread = args[++i];
                case "--grep" -> grep = args[++i];
                case "--tail" -> tail = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("csvview: no option " + args[i]);
                    System.exit(2);
                }
            }
        }

        // Loss first, and unconditionally. Every other answer here is computed from rows that are present, so a
        // reader who is not told about missing ones is being invited to conclude something from a hole.
        String loss = loss(rows);
        if (!loss.isEmpty()) {
            System.err.println(loss);
        }
        if (gaps) {
            System.out.print(gaps(rows, gapMillis));
            return;
        }
        List<Row> kept = filter(rows, lanes, kind, thread, grep);
        if (tail > 0 && kept.size() > tail) {
            kept = kept.subList(kept.size() - tail, kept.size());
        }
        for (Row r : kept) {
            System.out.println(r.raw());
        }
    }

    /** Parse a correlation log. Rows are returned in <b>time</b> order, which is the order to read them in. */
    public static List<Row> read(Path file) throws IOException {
        List<Row> rows = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("seq,")) {
                continue;
            }
            List<String> f = split(line);
            if (f.size() < 7) {
                continue;               // a torn last line: a run that was killed mid-write is the normal case
            }
            try {
                rows.add(new Row(Long.parseLong(f.get(0)), Long.parseLong(f.get(1)), f.get(2),
                        f.get(3), f.get(4), f.get(5), f.get(6), line));
            } catch (NumberFormatException e) {
                // Not a row. Skipping beats failing: the file is read while investigating something else.
            }
        }
        // Sorted on the monotonic clock, never on seq. Two threads can be numbered in an order the clock
        // disagrees with, and it is the clock that says what happened before what.
        rows.sort(Comparator.comparingLong(Row::monoNanos));
        return rows;
    }

    /**
     * Stretches with no {@code frame.present}, each judged against the park that precedes it.
     *
     * <p>The park is what makes the verdict possible. A loop that said it was going to sleep for 500ms and then
     * produced no frames for 480ms did exactly what it announced; the same silence with no park before it, or a
     * park far shorter than the gap, is the loop failing to come back.
     */
    public static String gaps(List<Row> rows, long thresholdMillis) {
        long threshold = thresholdMillis * 1_000_000L;
        StringBuilder sb = new StringBuilder();
        Row previousFrame = null;
        int found = 0;
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            if (!row.isFrame()) {
                continue;
            }
            if (previousFrame != null) {
                long gap = row.monoNanos() - previousFrame.monoNanos();
                if (gap >= threshold) {
                    found++;
                    sb.append(report(rows, previousFrame, row, gap));
                }
            }
            previousFrame = row;
        }
        if (found == 0) {
            return "no gaps of " + thresholdMillis + "ms or more between frames\n";
        }
        return sb.insert(0, found + " gap" + (found == 1 ? "" : "s")
                + " of " + thresholdMillis + "ms or more:\n\n").toString();
    }

    private static String report(List<Row> rows, Row from, Row to, long gap) {
        // The park that covers this gap is the one recorded with the frame that opened it: the loop presents,
        // says what budget it is parking on, and then sleeps.
        Row park = null;
        for (int i = rows.indexOf(from); i < rows.size() && rows.get(i).monoNanos() <= to.monoNanos(); i++) {
            if (rows.get(i).isPark()) {
                park = rows.get(i);
                break;
            }
        }
        String verdict;
        if (park == null) {
            verdict = "STALL - nothing said the loop was going to sleep";
        } else if (park.parkBudgetNanos() >= gap) {
            verdict = "expected - parked on " + park.detail();
        } else {
            verdict = "STALL - parked on " + park.detail() + ", which does not cover it";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "  %s for %.1fms  [%s]%n",
                from.wall(), gap / 1e6, verdict));
        sb.append("    from  ").append(from.raw()).append('\n');
        // The suspect: the last thing that happened before the silence. In a stall it is usually the cause,
        // which is the whole reason the log is one file in time order rather than several.
        Row last = null;
        for (Row r : rows) {
            if (r.monoNanos() > from.monoNanos() && r.monoNanos() < to.monoNanos()) {
                last = r;
            }
        }
        if (last != null) {
            sb.append("    last  ").append(last.raw()).append('\n');
        }
        sb.append("    to    ").append(to.raw()).append("\n\n");
        return sb.toString();
    }

    /** Missing sequence numbers — dropped rows, which every other answer here would otherwise hide. */
    public static String loss(List<Row> rows) {
        List<Row> bySeq = new ArrayList<>(rows);
        bySeq.sort(Comparator.comparingLong(Row::seq));
        long expected = -1;
        long missing = 0;
        for (Row r : bySeq) {
            if (expected >= 0 && r.seq() != expected) {
                missing += r.seq() - expected;
            }
            expected = r.seq() + 1;
        }
        return missing == 0 ? "" : "csvview: " + missing + " row(s) missing from this log; seq has gaps";
    }

    /**
     * The rows matching every criterion given; a {@code null} criterion is not applied.
     *
     * <p>Combined rather than alternative, because an investigation narrows: each option is an "and". Matching
     * is case-insensitive substring except for {@code lanes}, which is an exact comma-separated set — a lane is
     * a closed vocabulary and a partial match there would silently widen the answer.
     *
     * <p>Public because reading a run back is not only this command's job: a test that asserts what its own run
     * recorded is doing the same thing, and would otherwise reimplement it slightly differently.
     */
    public static List<Row> filter(List<Row> rows, String lanes, String kind, String thread, String grep) {
        List<String> wanted = lanes == null ? List.of() : List.of(lanes.toLowerCase(Locale.ROOT).split(","));
        List<Row> kept = new ArrayList<>();
        for (Row r : rows) {
            if (!wanted.isEmpty() && !wanted.contains(r.lane().toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (kind != null && !r.kind().toLowerCase(Locale.ROOT).contains(kind.toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (thread != null && !r.thread().toLowerCase(Locale.ROOT).contains(thread.toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (grep != null && !r.raw().toLowerCase(Locale.ROOT).contains(grep.toLowerCase(Locale.ROOT))) {
                continue;
            }
            kept.add(r);
        }
        return kept;
    }

    /** One row into fields, honouring RFC 4180 quoting — the writer's own rules, read back. */
    static List<String> split(String row) {
        List<String> out = new ArrayList<>(7);
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < row.length() && row.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        out.add(field.toString());
        return out;
    }
}
