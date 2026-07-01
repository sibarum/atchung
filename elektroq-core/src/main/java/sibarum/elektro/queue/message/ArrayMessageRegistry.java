package sibarum.elektro.queue.message;

import sibarum.elektro.queue.ElektroException;

import java.util.Objects;

/**
 * Default {@link MessageRegistry} backed by an open-addressing, int-keyed table.
 *
 * <p>Lookups on {@link #byId(int)} run on every inbound frame, so the table stores the
 * primitive {@code id} directly and probes linearly &mdash; no {@code Integer} boxing
 * and no per-lookup allocation. The table is sized once from an expected type count and
 * is intended to be filled at startup by generated registration code and then read
 * concurrently; concurrent {@link #register} calls must be externally synchronised.
 */
public final class ArrayMessageRegistry implements MessageRegistry {

    private static final float LOAD_FACTOR = 0.6f;

    private int[] ids;
    private MessageType<?>[] types;
    private int mask;
    private int size;
    private int resizeThreshold;

    public ArrayMessageRegistry() {
        this(16);
    }

    public ArrayMessageRegistry(int expectedTypes) {
        int capacity = tableSizeFor((int) (Math.max(expectedTypes, 1) / LOAD_FACTOR) + 1);
        allocate(capacity);
    }

    private void allocate(int capacity) {
        this.ids = new int[capacity];
        this.types = new MessageType<?>[capacity];
        this.mask = capacity - 1;
        this.resizeThreshold = (int) (capacity * LOAD_FACTOR);
    }

    private static int tableSizeFor(int n) {
        int cap = 1;
        while (cap < n) {
            cap <<= 1;
        }
        return Math.max(cap, 2);
    }

    private static int spread(int id) {
        // Fibonacci-style mix so sequential ids scatter across the table.
        int h = id * 0x9E3779B1;
        return h ^ (h >>> 16);
    }

    @Override
    public <T> void register(MessageType<T> type) {
        Objects.requireNonNull(type, "type");
        if (size >= resizeThreshold) {
            resize();
        }
        int id = type.id();
        int i = spread(id) & mask;
        while (types[i] != null) {
            if (ids[i] == id) {
                MessageType<?> existing = types[i];
                if (existing.equals(type)) {
                    return; // idempotent re-registration of the same descriptor
                }
                throw new ElektroException(
                        "Message id " + id + " already registered to " + existing.name()
                                + ", cannot rebind to " + type.name());
            }
            i = (i + 1) & mask;
        }
        ids[i] = id;
        types[i] = type;
        size++;
    }

    private void resize() {
        MessageType<?>[] old = types;
        allocate(types.length << 1);
        size = 0;
        for (MessageType<?> t : old) {
            if (t != null) {
                register(t);
            }
        }
    }

    @Override
    public MessageType<?> byId(int id) {
        int i = spread(id) & mask;
        MessageType<?> t;
        while ((t = types[i]) != null) {
            if (ids[i] == id) {
                return t;
            }
            i = (i + 1) & mask;
        }
        return null;
    }

    @Override
    public boolean contains(int id) {
        return byId(id) != null;
    }

    @Override
    public int size() {
        return size;
    }
}
