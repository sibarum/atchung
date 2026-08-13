package sibarum.elektro.queue.netcode.channel;

import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Packs and unpacks the list of {@link NetMessage}s carried in one DATA packet's payload
 * (docs/netcode-design.md). Hand-written and reflection-free, like the rest of elektro-Q.
 *
 * <p>Layout: a var-int count, then each message as
 * {@code channelId(1) | mode(1) | sequence(4) | length(varint) | bytes}. Packing several small
 * messages into one datagram is what keeps per-message overhead low and lets unacked reliable
 * messages be coalesced with new traffic on the next send.
 */
public final class MessageCodec {

    /** Fixed per-message header bytes (channelId + mode + sequence), excluding the var-int length. */
    public static final int FRAGMENT_OVERHEAD = 1 + 1 + 4;

    private MessageCodec() {}

    /** Encodes {@code messages} into a single packet payload. */
    public static byte[] encode(List<NetMessage> messages) {
        WireBufferWriter out = new WireBufferWriter();
        out.putVarInt(messages.size());
        for (NetMessage m : messages) {
            out.putByte((byte) m.channelId());
            out.putByte((byte) m.mode().code());
            out.putInt(m.sequence());
            out.putVarInt(m.payload().length);
            out.putBytes(m.payload());
        }
        return out.toByteArray();
    }

    /** Decodes a packet payload back into its messages. */
    public static List<NetMessage> decode(byte[] payload) {
        WireBufferReader in = new WireBufferReader(payload);
        int count = in.getVarInt();
        List<NetMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int channelId = in.getByte() & 0xFF;
            DeliveryMode mode = DeliveryMode.fromCode(in.getByte() & 0xFF);
            int sequence = in.getInt();
            int length = in.getVarInt();
            byte[] bytes = in.getBytes(length);
            messages.add(new NetMessage(channelId, mode, sequence, bytes));
        }
        return messages;
    }
}
