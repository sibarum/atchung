package sibarum.elektro.queue.message;

import sibarum.intrabuf.Compat;
import sibarum.intrabuf.SchemaInfo;
import sibarum.intrabuf.annotation.Field;
import sibarum.intrabuf.annotation.IntrabufHeader;
import sibarum.intrabuf.annotation.Role;

import static sibarum.intrabuf.HeaderRole.COMPAT;
import static sibarum.intrabuf.HeaderRole.FINGERPRINT;
import static sibarum.intrabuf.HeaderRole.MAJOR;
import static sibarum.intrabuf.HeaderRole.MINOR;
import static sibarum.intrabuf.HeaderRole.SCHEMA_ID;
import static sibarum.intrabuf.HeaderRole.SNAPSHOT;
import static sibarum.intrabuf.HeaderRole.WIRE_VERSION;

/**
 * The envelope prefixing every frame on the wire.
 *
 * <p>It is an intrabuf {@code @IntrabufHeader}: the components marked {@code @Role} carry intrabuf's schema facts
 * (type id, version, compatibility mode, snapshot flag, structural fingerprint), so the receiving conduit can route
 * on {@link #typeId()} and then let intrabuf decide whether the payload is one this build may decode. The rest is
 * elektro-Q's own: the {@link Protocol} guard values, the {@link Flags} as packed bits, the correlation id, and the
 * {@code sibarum.probe.Cause} the frame was sent in.
 *
 * <p><b>The cause is negotiated, not assumed.</b> A peer from before it reads a header's fields with no way to skip
 * one it does not know, so a cause sent to it would be read as the start of the payload. Every frame therefore says
 * whether its sender reads causes ({@link #causes()}, a bit an older peer ignores), and a {@link #cause()}, the only
 * part with bytes of its own, goes only to a peer that has said so. Without it the header is the one version 2
 * always had.
 * {@code EnvelopeIntrabuf} (generated) reads and writes it; see {@link Protocol} for the resulting layout.
 *
 * <p>There is no payload length: every transport already delimits frames, and the payload is the rest of the frame.
 */
@IntrabufHeader(name = "elektroq.Envelope")
public record Envelope(
        short magic,
        byte protocolVersion,
        @Role(WIRE_VERSION) byte wireVersion,
        @Role(SCHEMA_ID) int typeId,
        @Role(MAJOR) short major,
        @Role(MINOR) short minor,
        @Role(COMPAT) Compat compat,
        @Role(SNAPSHOT) boolean snapshot,
        @Role(FINGERPRINT) @Field(fixed = true) Integer fingerprint,
        boolean request,
        boolean reply,
        boolean fragment,
        boolean compressed,
        long correlationId,
        boolean causes,
        @Field(fixed = true) Long cause) {

    private static final SchemaInfo HELLO = new SchemaInfo("elektroq.Hello", "elektroq.Hello", Protocol.HELLO_TYPE, 1, 0,
            Compat.FULL, 0, false, "elektroq");

    /** The envelope for a frame of a message with schema {@code schema}, sent in no cause. */
    public static Envelope of(SchemaInfo schema, byte flags, long correlationId) {
        return of(schema, flags, correlationId, 0L);
    }

    /**
     * The envelope for a frame of a message with schema {@code schema}, carrying {@code cause} unless it is
     * {@code Cause.NONE}. Only for a peer that reads causes: see the class comment.
     */
    public static Envelope of(SchemaInfo schema, byte flags, long correlationId, long cause) {
        return EnvelopeIntrabuf.create(schema, Protocol.MAGIC, (byte) Protocol.VERSION,
                Flags.has(flags, Flags.REQUEST), Flags.has(flags, Flags.REPLY),
                Flags.has(flags, Flags.FRAGMENT), Flags.has(flags, Flags.COMPRESSED), correlationId,
                true, cause == 0L ? null : cause);
    }

    /**
     * The hello a conduit sends each peer as it connects: {@link Protocol#HELLO_TYPE}, no payload, and this
     * envelope's {@link #causes()} bit, so the peer can send causes before it has had anything else from here. A
     * peer from before causes looks the type up, finds nothing, and drops the frame, as it does any type it does
     * not know.
     */
    public static Envelope hello() {
        return EnvelopeIntrabuf.create(HELLO, Protocol.MAGIC, (byte) Protocol.VERSION, false, false, false, false, 0L,
                true, null);
    }

    /** The cause this frame was sent in, or {@code Cause.NONE}. */
    public long causeId() {
        return cause == null ? 0L : cause;
    }

    /** The {@link Flags} as a byte. */
    public byte flags() {
        return (byte) ((request ? Flags.REQUEST : 0) | (reply ? Flags.REPLY : 0)
                | (fragment ? Flags.FRAGMENT : 0) | (compressed ? Flags.COMPRESSED : 0));
    }
}
