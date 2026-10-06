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
 * elektro-Q's own: the {@link Protocol} guard values, the {@link Flags} as packed bits, and the correlation id.
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
        long correlationId) {

    /** The envelope for a frame of a message with schema {@code schema}. */
    public static Envelope of(SchemaInfo schema, byte flags, long correlationId) {
        return EnvelopeIntrabuf.create(schema, Protocol.MAGIC, (byte) Protocol.VERSION,
                Flags.has(flags, Flags.REQUEST), Flags.has(flags, Flags.REPLY),
                Flags.has(flags, Flags.FRAGMENT), Flags.has(flags, Flags.COMPRESSED), correlationId);
    }

    /** The {@link Flags} as a byte. */
    public byte flags() {
        return (byte) ((request ? Flags.REQUEST : 0) | (reply ? Flags.REPLY : 0)
                | (fragment ? Flags.FRAGMENT : 0) | (compressed ? Flags.COMPRESSED : 0));
    }
}
