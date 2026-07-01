package sibarum.elektro.queue.message;

/**
 * Wire-level protocol constants shared by every elektro-Q transport.
 *
 * <p>Every frame begins with a fixed-size {@linkplain MessageHeader header} whose
 * layout is defined here. A fixed header (rather than a var-int one) lets a transport
 * read exactly {@link #HEADER_BYTES} bytes, learn the {@code payloadLength}, and then
 * read the payload in a second, precisely-sized step &mdash; cheap to frame and easy
 * to reason about across TCP, UDP, or shared memory.
 *
 * <pre>
 *   offset  size  field
 *   0       2     magic          (0xEC51)
 *   2       1     protocolVersion
 *   3       1     flags
 *   4       4     typeId
 *   8       4     schemaVersion
 *   12      8     correlationId
 *   20      4     payloadLength
 *   ----    ----
 *   24            HEADER_BYTES
 * </pre>
 */
public final class Protocol {

    /** Frame magic used to detect desynchronised or foreign streams. */
    public static final short MAGIC = (short) 0xEC51;

    /** Version of the framing/handshake protocol itself (distinct from message schemas). */
    public static final int VERSION = 1;

    /** Fixed size, in bytes, of the frame header. */
    public static final int HEADER_BYTES = 24;

    private Protocol() {
    }
}
