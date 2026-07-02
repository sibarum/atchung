package sibarum.elektro.queue.netcode.channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The receive side of the channel layer: applies each message's {@link DeliveryMode} to decide what
 * to deliver, in what order, and what to drop (docs/netcode-design.md). State is per channel and
 * independent, so ordering work on one channel never blocks another.
 *
 * <ul>
 *   <li><b>Unreliable</b> — delivered as it arrives.</li>
 *   <li><b>Unreliable-sequenced</b> — delivered only if newer than the newest seen (stale dropped).</li>
 *   <li><b>Reliable-unordered</b> — delivered immediately, duplicates suppressed.</li>
 *   <li><b>Reliable-ordered</b> — buffered and released strictly in sequence (this is the only mode
 *       that holds a message waiting for an earlier one).</li>
 * </ul>
 */
public final class ChannelReceiver {

    private final Map<Integer, PerChannel> channels = new HashMap<>();

    /** Applies mode logic to {@code incoming}, returning the messages to deliver, in delivery order. */
    public List<NetMessage> accept(List<NetMessage> incoming) {
        List<NetMessage> delivered = new ArrayList<>();
        for (NetMessage m : incoming) {
            channels.computeIfAbsent(m.channelId(), id -> new PerChannel()).receive(m, delivered);
        }
        return delivered;
    }

    private static final class PerChannel {
        // UNRELIABLE_SEQUENCED: highest sequence delivered so far.
        private int highestSequenced = -1;
        // RELIABLE_ORDERED: next sequence to release, plus a reorder buffer for early arrivals.
        private int nextOrdered = 0;
        private final Map<Integer, NetMessage> orderedBuffer = new HashMap<>();
        // RELIABLE_UNORDERED: low-water mark of contiguous delivered + the set delivered above it.
        private int contiguous = 0;
        private final Set<Integer> deliveredAbove = new HashSet<>();

        void receive(NetMessage m, List<NetMessage> out) {
            switch (m.mode()) {
                case UNRELIABLE -> out.add(m);
                case UNRELIABLE_SEQUENCED -> {
                    if (m.sequence() > highestSequenced) {
                        highestSequenced = m.sequence();
                        out.add(m);
                    }
                }
                case RELIABLE_UNORDERED -> receiveUnordered(m, out);
                case RELIABLE_ORDERED -> receiveOrdered(m, out);
            }
        }

        private void receiveUnordered(NetMessage m, List<NetMessage> out) {
            int s = m.sequence();
            if (s < contiguous || deliveredAbove.contains(s)) {
                return; // duplicate
            }
            out.add(m); // deliver immediately, order-independent
            if (s == contiguous) {
                contiguous++;
                while (deliveredAbove.remove(contiguous)) {
                    contiguous++;
                }
            } else {
                deliveredAbove.add(s);
            }
        }

        private void receiveOrdered(NetMessage m, List<NetMessage> out) {
            int s = m.sequence();
            if (s < nextOrdered) {
                return; // already delivered
            }
            if (s == nextOrdered) {
                out.add(m);
                nextOrdered++;
                NetMessage buffered;
                while ((buffered = orderedBuffer.remove(nextOrdered)) != null) {
                    out.add(buffered);
                    nextOrdered++;
                }
            } else {
                orderedBuffer.putIfAbsent(s, m); // early arrival; wait for the gap to fill
            }
        }
    }
}
