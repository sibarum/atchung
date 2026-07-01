package sibarum.elektro.queue.message;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrayMessageRegistryTest {

    /** Minimal no-op codec; registry tests never actually serialize. */
    private static final Codec<String> NOOP = new Codec<>() {
        @Override public void encode(String value, WireWriter out) { }
        @Override public String decode(WireReader in) { return null; }
    };

    private static MessageType<String> type(int id) {
        return new MessageType<>(id, 1, "T" + id, String.class, NOOP);
    }

    @Test
    void registersAndLooksUpById() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry();
        MessageType<String> t = type(7);
        registry.register(t);

        assertSame(t, registry.byId(7));
        assertTrue(registry.contains(7));
        assertEquals(1, registry.size());
    }

    @Test
    void missingIdReturnsNull() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry();
        assertNull(registry.byId(999));
        assertFalse(registry.contains(999));
    }

    @Test
    void handlesZeroAndNegativeIds() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry();
        registry.register(type(0));
        registry.register(type(-5));
        assertSame(registry.byId(0), registry.byId(0));
        assertEquals("T0", registry.byId(0).name());
        assertEquals("T-5", registry.byId(-5).name());
    }

    @Test
    void duplicateIdWithDifferentTypeThrows() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry();
        registry.register(type(3));
        MessageType<String> clashing = new MessageType<>(3, 2, "Other", String.class, NOOP);
        assertThrows(ElektroException.class, () -> registry.register(clashing));
    }

    @Test
    void reRegisteringIdenticalDescriptorIsIdempotent() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry();
        MessageType<String> t = type(3);
        registry.register(t);
        registry.register(t);
        assertEquals(1, registry.size());
    }

    @Test
    void survivesResizeWithManySequentialIds() {
        ArrayMessageRegistry registry = new ArrayMessageRegistry(4);
        for (int id = 0; id < 500; id++) {
            registry.register(type(id));
        }
        assertEquals(500, registry.size());
        for (int id = 0; id < 500; id++) {
            assertEquals("T" + id, registry.byId(id).name(), "id " + id + " after resize");
        }
    }
}
