package sibarum.elektro.queue.message;

/**
 * Wire-level protocol constants shared by every elektro-Q transport.
 *
 * <p>Every frame is an {@linkplain Envelope envelope} followed by the payload. The envelope is encoded by intrabuf as
 * a record body: a bitset first (the snapshot bit, the fingerprint's presence bit, and the four {@link Flags}), then
 * each field in order, integers as zigzag varints unless fixed:
 *
 * <pre>
 *   size    field
 *   1       bitset           snapshot, fingerprint present, request, reply, fragment, compressed,
 *                            reads causes, cause present
 *   1-3     magic            (0xEC51)
 *   1       protocolVersion
 *   1       wireVersion      intrabuf's body format
 *   1-5     typeId
 *   1-3     major
 *   1-3     minor
 *   1       compat           STRICT, BACKWARD or FULL
 *   0 or 4  fingerprint      when the message's schema calls for one
 *   1-10    correlationId
 *   0 or 8  cause            only to a peer that reads causes
 * </pre>
 *
 * The last two bits and the cause came later, within version 2: a peer from before them ignores bits it does not
 * know, and is never sent the cause, whose bytes it could not skip. See {@link Envelope}.
 *
 * A one-way emit of a small-id message is 10 bytes; version 1's fixed header was 24. Transports already delimit
 * frames (TCP and UDS with a length prefix, UDP by datagram and reassembly), so the envelope carries no payload
 * length: the payload is the rest of the frame.
 */
public final class Protocol {

    /** Frame magic used to detect desynchronised or foreign streams. */
    public static final short MAGIC = (short) 0xEC51;

    /**
     * Version of the framing protocol itself (distinct from message schemas). Version 2 is the intrabuf envelope;
     * version 1 was a fixed 24-byte big-endian header.
     */
    public static final int VERSION = 2;

    /**
     * The type id of a conduit's {@linkplain Envelope#hello() hello}, which no registry may give a type: an older
     * peer finds nothing under it and drops the frame.
     */
    public static final int HELLO_TYPE = -1;

    private Protocol() {
    }
}
