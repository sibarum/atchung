package sibarum.elektro.queue.message;

import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

/**
 * Hand-written {@link Codec} for the frame {@link MessageHeader}.
 *
 * <p>This is the one codec elektro-Q ships by hand rather than generating: it defines
 * the framing every transport relies on, and it doubles as a worked example of the
 * layout that generated message codecs follow. It writes and validates the
 * {@link Protocol#MAGIC} and {@link Protocol#VERSION} guard bytes around the header
 * fields defined in {@link Protocol}.
 *
 * <p>The instance is stateless; use {@link #INSTANCE}.
 */
public final class HeaderCodec implements Codec<MessageHeader> {

    public static final HeaderCodec INSTANCE = new HeaderCodec();

    private HeaderCodec() {
    }

    @Override
    public void encode(MessageHeader header, WireWriter out) {
        out.putShort(Protocol.MAGIC)
           .putByte((byte) Protocol.VERSION)
           .putByte(header.flags())
           .putInt(header.typeId())
           .putInt(header.schemaVersion())
           .putLong(header.correlationId())
           .putInt(header.payloadLength());
    }

    @Override
    public MessageHeader decode(WireReader in) {
        short magic = in.getShort();
        if (magic != Protocol.MAGIC) {
            throw new ElektroException("Bad frame magic: 0x" + Integer.toHexString(magic & 0xFFFF));
        }
        int version = in.getByte() & 0xFF;
        if (version != Protocol.VERSION) {
            throw new ElektroException("Unsupported protocol version: " + version);
        }
        byte flags = in.getByte();
        int typeId = in.getInt();
        int schemaVersion = in.getInt();
        long correlationId = in.getLong();
        int payloadLength = in.getInt();
        return new MessageHeader(typeId, schemaVersion, flags, correlationId, payloadLength);
    }
}
