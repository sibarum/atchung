package sibarum.elektro.queue.transport.local;

import sibarum.elektro.queue.ElektroException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The process-wide rendezvous for {@link LocalTransport}s: it maps a string endpoint to the
 * listening transport bound there, so a dialing transport can find its counterpart within the JVM.
 *
 * <p>This is the in-VM analogue of the OS TCP port table. There is a single {@link #INSTANCE};
 * endpoints are arbitrary caller-chosen names and must be unique among live listeners.
 */
final class LocalSwitch {

    static final LocalSwitch INSTANCE = new LocalSwitch();

    private final ConcurrentHashMap<String, LocalTransport> listeners = new ConcurrentHashMap<>();

    private LocalSwitch() {}

    /** Publishes {@code listener} at {@code endpoint}; rejects a name already in use. */
    void bind(String endpoint, LocalTransport listener) {
        LocalTransport previous = listeners.putIfAbsent(endpoint, listener);
        if (previous != null && previous != listener) {
            throw new ElektroException("Local endpoint already bound: '" + endpoint + "'");
        }
    }

    /** The listener at {@code endpoint}, or {@code null} if none is bound. */
    LocalTransport dial(String endpoint) {
        return listeners.get(endpoint);
    }

    /** Removes {@code listener} from {@code endpoint} (only if it is still the bound one). */
    void unbind(String endpoint, LocalTransport listener) {
        listeners.remove(endpoint, listener);
    }
}
