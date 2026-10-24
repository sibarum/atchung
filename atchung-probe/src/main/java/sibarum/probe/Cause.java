package sibarum.probe;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What set the current work off: a correlation ID, carried on the thread, through the bus and into every probe row.
 * See {@code docs/correlation.md}.
 *
 * <p><b>Causes begin where the outside world comes in</b>, at a {@link Source}: an input backend, a timer, the
 * automation server. {@link #begin(Source, Runnable)} runs its body inside the source's next cause, and everything the
 * body does is done in it. The bus stores the cause beside each event it queues and re-enters it around the handler,
 * so a cause passes through any number of mailboxes with nobody passing it along.
 *
 * <p><b>The ID is one {@code long}</b>, so nothing is allocated to make, carry or store one:
 * <pre>
 *   20 bits   the process, random at startup and never 0
 *    4 bits   the kind of source ({@link Kind})
 *   10 bits   the source's index, given when it registers
 *   30 bits   the source's sequence number; 0 is its default cause
 * </pre>
 * Written {@code 3f2a1.t07.4f2} by {@link #format}. {@link #NONE}, the whole {@code long} 0, is no cause.
 *
 * <p><b>Held in a per-thread slot, not a {@link ScopedValue}.</b> A scoped value would be the natural carrier, but
 * binding one allocates, and a pumped mailbox's drain re-enters a cause per event while promising to allocate nothing.
 * The slot gives the same guarantee a scoped value does by having no setter: {@link #within} and {@link #begin} put
 * the previous cause back when their body ends, and {@link #enter} / {@link #exit}, for message runtimes, must be
 * paired in a {@code finally}. So a cause cannot outlive the work it was entered for, and a thread that handles the
 * next message does not inherit the last one's.
 *
 * <p>This class must not touch {@link Probe} while it initialises: {@code Probe}'s own initialiser asks it for the
 * process's prefix.
 */
public final class Cause {

    /** No cause: work nobody wrapped. */
    public static final long NONE = 0L;

    private static final int SOURCE_BITS = 10;
    private static final int SEQUENCE_BITS = 30;
    private static final int SOURCE_SHIFT = SEQUENCE_BITS;
    private static final int KIND_SHIFT = SOURCE_SHIFT + SOURCE_BITS;
    private static final int PROCESS_SHIFT = KIND_SHIFT + 4;
    private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1;
    private static final int SOURCE_LIMIT = 1 << SOURCE_BITS;
    /** The last index, kept for every source that registers after the others are taken. */
    private static final int OVERFLOW_INDEX = SOURCE_LIMIT - 1;

    /** This process's prefix: random, 20 bits, never 0, so no cause of any source is ever {@link #NONE}. */
    private static final int PROCESS = ThreadLocalRandom.current().nextInt(1, 1 << 20);

    private static final ThreadLocal<long[]> CURRENT = ThreadLocal.withInitial(() -> new long[1]);

    private static final ConcurrentHashMap<String, Source> SOURCES = new ConcurrentHashMap<>();
    private static final AtomicInteger NEXT_INDEX = new AtomicInteger();
    private static final Source OVERFLOW = new Source("(overflow)", Kind.OTHER, OVERFLOW_INDEX);

    private Cause() {
    }

    /** What kind of thing a source is. Its letter is what {@link #format} writes. */
    public enum Kind {
        INPUT('i'), TIMER('t'), COMMAND('c'), IO('o'), MAILBOX('m'), BOOT('b'), OTHER('x');

        private final char letter;

        Kind(char letter) {
            this.letter = letter;
        }

        /** The letter a formatted cause shows for this kind. */
        public char letter() {
            return letter;
        }

        static Kind of(char letter) {
            for (Kind k : values()) {
                if (k.letter == letter) {
                    return k;
                }
            }
            return null;
        }
    }

    /**
     * Something that begins causes. Registered once by name, and the same object for the life of the process, so
     * a source asked for twice under one name and kind is one source. Its causes share its prefix and differ by
     * sequence number, so a timer's firings are told apart and still grouped by the timer.
     */
    public static final class Source {
        private final String name;
        private final Kind kind;
        private final int index;
        private final AtomicLong sequence = new AtomicLong();

        private Source(String name, Kind kind, int index) {
            this.name = name;
            this.kind = kind;
            this.index = index;
        }

        /** The name it registered under. */
        public String name() {
            return name;
        }

        public Kind kind() {
            return kind;
        }

        /** Its index within this process, {@code 0..1023}. */
        public int index() {
            return index;
        }

        /** What a formatted cause shows for it: its kind's letter and index, {@code t07}. */
        public String key() {
            return sourceKey(kind, index);
        }

        /** Its default cause, sequence 0: for what it does on its own initiative rather than because something arrived. */
        public long defaultCause() {
            return id(kind, index, 0L);
        }

        /**
         * A new cause of this source's: the next sequence number. After a billion it wraps, skipping 0, which is the
         * default's.
         */
        public long next() {
            long s = sequence.incrementAndGet() & SEQUENCE_MASK;
            if (s == 0L) {
                s = sequence.incrementAndGet() & SEQUENCE_MASK;
            }
            return id(kind, index, s);
        }

        @Override
        public String toString() {
            return key() + " " + name;
        }
    }

    /**
     * The source called {@code name}, registered the first time it is asked for. Registering writes a
     * {@code cause.source} row, so a reader can resolve {@code t07} to a name. Past 1,023 sources every further one
     * shares the last index, under the name {@code (overflow)}, and says so once on the log: causes stay distinct by
     * sequence, but a source can no longer be told from the others by its key.
     */
    public static Source source(String name, Kind kind) {
        String key = kind.letter + name;
        Source existing = SOURCES.get(key);
        if (existing != null) {
            return existing;
        }
        boolean[] made = new boolean[1];
        Source s = SOURCES.computeIfAbsent(key, k -> {
            int index = NEXT_INDEX.getAndIncrement();
            if (index >= OVERFLOW_INDEX) {
                return OVERFLOW;
            }
            made[0] = true;
            return new Source(name, kind, index);
        });
        if (made[0]) {
            Probe.note(Lane.APP, "cause.source", s.key() + " " + name);
        } else if (s == OVERFLOW && NEXT_INDEX.get() == OVERFLOW_INDEX + 1) {
            Log.of("cause").warn("more than " + OVERFLOW_INDEX + " cause sources; '" + name
                    + "' and every source after it share " + OVERFLOW.key());
        }
        return s;
    }

    /** The cause this thread is working in, or {@link #NONE}. */
    public static long current() {
        return CURRENT.get()[0];
    }

    /** Run {@code body} inside {@code source}'s next cause. */
    public static void begin(Source source, Runnable body) {
        within(source.next(), body);
    }

    /** Run {@code body} inside {@code cause}, which was carried from elsewhere, and put back the one this thread was in. */
    public static void within(long cause, Runnable body) {
        long[] slot = CURRENT.get();
        long previous = slot[0];
        slot[0] = cause;
        try {
            body.run();
        } finally {
            slot[0] = previous;
        }
    }

    /**
     * Enter {@code cause} and return the one this thread was in, for a message runtime that cannot wrap its handler
     * in a lambda without allocating. Always paired with {@link #exit} in a {@code finally}:
     * <pre>
     *   long previous = Cause.enter(cause);
     *   try { handler.on(event); } finally { Cause.exit(previous); }
     * </pre>
     */
    public static long enter(long cause) {
        long[] slot = CURRENT.get();
        long previous = slot[0];
        slot[0] = cause;
        return previous;
    }

    /** Put back the cause {@link #enter} returned. */
    public static void exit(long previous) {
        CURRENT.get()[0] = previous;
    }

    /** This process's prefix, the high 20 bits of every cause it begins. */
    public static int process() {
        return PROCESS;
    }

    /** This process, as the probe's first row and the log's header introduce it: its prefix and pid. */
    public static String describeProcess() {
        return String.format(Locale.ROOT, "%05x pid=%d", PROCESS, ProcessHandle.current().pid());
    }

    public static int processOf(long cause) {
        return (int) (cause >>> PROCESS_SHIFT);
    }

    public static Kind kindOf(long cause) {
        int k = (int) (cause >>> KIND_SHIFT) & 0xF;
        return k < Kind.values().length ? Kind.values()[k] : null;
    }

    public static int sourceOf(long cause) {
        return (int) (cause >>> SOURCE_SHIFT) & (SOURCE_LIMIT - 1);
    }

    public static long sequenceOf(long cause) {
        return cause & SEQUENCE_MASK;
    }

    /** {@code 3f2a1.t07.4f2}: process in hex, kind and source index, sequence in hex. {@link #NONE} is {@code ""}. */
    public static String format(long cause) {
        if (cause == NONE) {
            return "";
        }
        Kind kind = kindOf(cause);
        return String.format(Locale.ROOT, "%05x.%c%02d.%x", processOf(cause), kind == null ? '?' : kind.letter,
                sourceOf(cause), sequenceOf(cause));
    }

    /** The cause {@link #format} wrote, or {@link #NONE} for {@code ""} or anything that is not one. */
    public static long parse(String text) {
        if (text == null || text.isEmpty()) {
            return NONE;
        }
        String[] parts = text.split("\\.");
        if (parts.length != 3 || parts[1].length() < 2) {
            return NONE;
        }
        Kind kind = Kind.of(parts[1].charAt(0));
        if (kind == null) {
            return NONE;
        }
        try {
            long process = Long.parseLong(parts[0], 16);
            int index = Integer.parseInt(parts[1].substring(1));
            long sequence = Long.parseLong(parts[2], 16);
            if (process <= 0 || process >= 1 << 20 || index < 0 || index >= SOURCE_LIMIT
                    || sequence < 0 || sequence > SEQUENCE_MASK) {
                return NONE;
            }
            return process << PROCESS_SHIFT | (long) kind.ordinal() << KIND_SHIFT | (long) index << SOURCE_SHIFT
                    | sequence;
        } catch (NumberFormatException e) {
            return NONE;
        }
    }

    private static long id(Kind kind, int index, long sequence) {
        return (long) PROCESS << PROCESS_SHIFT | (long) kind.ordinal() << KIND_SHIFT
                | (long) index << SOURCE_SHIFT | sequence;
    }

    private static String sourceKey(Kind kind, int index) {
        return String.format(Locale.ROOT, "%c%02d", kind.letter, index);
    }
}
