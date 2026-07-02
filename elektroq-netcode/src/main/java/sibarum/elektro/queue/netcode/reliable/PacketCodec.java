package sibarum.elektro.queue.netcode.reliable;

import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

/**
 * The hand-written codec for the netcode packet header, prepended to every datagram this layer sends
 * (docs/netcode-design.md, layer 2). Reflection-free, like every elektro-Q codec.
 *
 * <p>Layout — a fixed 13-byte header, then the payload:
 * <pre>
 *   protocolId(4) | type(1) | sequence(2) | ack(2) | ackBits(4) | payload...
 * </pre>
 * {@code protocolId} lets a receiver reject stray or mismatched-version datagrams before trusting
 * anything else; {@code sequence} is this packet's own number; {@code ack}/{@code ackBits} are the
 * receiver's feedback about the <em>peer's</em> recent packets (latest received + a bitfield of the
 * 32 before it). Multi-byte integers are big-endian, matching the message envelope.
 */
public final class PacketCodec {

    /** Header size in bytes. */
    public static final int HEADER_BYTES = 13;

    private PacketCodec() {}

    /** Encodes {@code header fields + payload} into a single datagram byte array. */
    public static byte[] encode(int protocolId, int type, int sequence, int ack, int ackBits, byte[] payload) {
        WireBufferWriter out = new WireBufferWriter(HEADER_BYTES + payload.length);
        out.putInt(protocolId);
        out.putByte((byte) type);
        out.putShort((short) sequence);
        out.putShort((short) ack);
        out.putInt(ackBits);
        out.putBytes(payload);
        return out.toByteArray();
    }

    /**
     * Decodes a datagram, or returns {@code null} if it is too short or its protocol id does not
     * match {@code expectedProtocolId} (a stray or foreign packet — dropped, not trusted).
     */
    public static Packet decode(byte[] datagram, int expectedProtocolId) {
        if (datagram.length < HEADER_BYTES) {
            return null;
        }
        WireBufferReader in = new WireBufferReader(datagram);
        int protocolId = in.getInt();
        if (protocolId != expectedProtocolId) {
            return null;
        }
        int type = in.getByte() & 0xFF;
        int sequence = in.getShort() & 0xFFFF;
        int ack = in.getShort() & 0xFFFF;
        int ackBits = in.getInt();
        byte[] payload = in.getBytes(in.remaining());
        return new Packet(type, sequence, ack, ackBits, payload);
    }
}
