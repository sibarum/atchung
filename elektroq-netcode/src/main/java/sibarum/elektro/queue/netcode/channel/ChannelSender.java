package sibarum.elektro.queue.netcode.channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The send side of the channel layer: assigns per-channel sequence numbers, and decides which
 * messages go into the next packet (docs/netcode-design.md).
 *
 * <p>Unreliable messages are packed once and forgotten. Reliable messages are retained until the
 * packet that carried them is acknowledged: {@link #pack} includes a reliable message when it is new
 * or when its last send is older than the retransmit timeout, and {@link #acked} drops it once its
 * carrying packet is confirmed. This is the "resend the unacked <em>messages</em>, coalesced into the
 * next packet" model — the reliability is driven by the packet acks from the layer below, so there
 * is no second ack scheme on the wire.
 *
 * <p>Not thread-safe; the owning session confines it under its lock.
 */
public final class ChannelSender {

    private final Map<Integer, Integer> nextSequence = new HashMap<>();
    private final List<NetMessage> unreliable = new ArrayList<>();
    private final LinkedHashMap<Long, Pending> reliable = new LinkedHashMap<>();

    /** Queues a message, assigning its per-channel sequence. */
    public void enqueue(int channelId, DeliveryMode mode, byte[] payload) {
        int seq = nextSequence.merge(channelId, 1, Integer::sum) - 1; // 0, 1, 2, ...
        NetMessage message = new NetMessage(channelId, mode, seq, payload);
        if (mode.reliable()) {
            reliable.put(key(channelId, seq), new Pending(message));
        } else {
            unreliable.add(message);
        }
    }

    /**
     * Selects the messages for the packet numbered {@code packetSeq}: all queued unreliable messages
     * (once), plus reliable messages that are new or overdue for retransmit, up to {@code budgetBytes}.
     * Records each selected reliable message as carried by {@code packetSeq}.
     */
    public List<NetMessage> pack(int packetSeq, long nowNanos, long rtoNanos, int budgetBytes) {
        List<NetMessage> out = new ArrayList<>();
        int used = 0;

        Iterator<NetMessage> it = unreliable.iterator();
        while (it.hasNext()) {
            NetMessage m = it.next();
            int size = sizeOf(m);
            if (used + size > budgetBytes) {
                break;
            }
            out.add(m);
            used += size;
            it.remove();
        }

        for (Pending p : reliable.values()) {
            boolean due = p.lastPacketSeq < 0 || nowNanos - p.lastSentNanos > rtoNanos;
            if (!due) {
                continue;
            }
            int size = sizeOf(p.message);
            if (used + size > budgetBytes) {
                continue; // try to fit smaller ones; this one waits for the next packet
            }
            out.add(p.message);
            used += size;
            p.lastPacketSeq = packetSeq;
            p.lastSentNanos = nowNanos;
        }
        return out;
    }

    /** Marks reliable messages carried by {@code packetSeq} as delivered (removes them). */
    public void acked(int packetSeq) {
        reliable.values().removeIf(p -> p.lastPacketSeq == packetSeq);
    }

    /** Whether there is anything to send right now (new, or a reliable message overdue). */
    public boolean hasWork(long nowNanos, long rtoNanos) {
        if (!unreliable.isEmpty()) {
            return true;
        }
        for (Pending p : reliable.values()) {
            if (p.lastPacketSeq < 0 || nowNanos - p.lastSentNanos > rtoNanos) {
                return true;
            }
        }
        return false;
    }

    private static int sizeOf(NetMessage m) {
        return MessageCodec.FRAGMENT_OVERHEAD + 3 + m.payload().length; // + up to 3 bytes var-int length
    }

    private static long key(int channelId, int sequence) {
        return ((long) channelId << 32) | (sequence & 0xFFFFFFFFL);
    }

    private static final class Pending {
        final NetMessage message;
        int lastPacketSeq = -1;
        long lastSentNanos = 0;

        Pending(NetMessage message) {
            this.message = message;
        }
    }
}
