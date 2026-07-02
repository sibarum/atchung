package sibarum.elektro.queue.netcode.channel;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic tests of the codec, the per-mode receive semantics, and the retransmit rule. */
class ChannelLogicTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static NetMessage msg(int channel, DeliveryMode mode, int seq, String payload) {
        return new NetMessage(channel, mode, seq, bytes(payload));
    }

    private static List<String> texts(List<NetMessage> ms) {
        return ms.stream().map(m -> new String(m.payload(), StandardCharsets.UTF_8)).toList();
    }

    @Test
    void codecRoundTripsAContainer() {
        List<NetMessage> in = List.of(
                msg(0, DeliveryMode.RELIABLE_ORDERED, 7, "a"),
                msg(3, DeliveryMode.UNRELIABLE, 0, ""),
                msg(1, DeliveryMode.UNRELIABLE_SEQUENCED, 42, "position"));
        List<NetMessage> out = MessageCodec.decode(MessageCodec.encode(in));
        assertEquals(in.size(), out.size());
        for (int i = 0; i < in.size(); i++) {
            assertEquals(in.get(i).channelId(), out.get(i).channelId());
            assertEquals(in.get(i).mode(), out.get(i).mode());
            assertEquals(in.get(i).sequence(), out.get(i).sequence());
            assertArrayEquals(in.get(i).payload(), out.get(i).payload());
        }
    }

    @Test
    void unreliableDeliversEverythingAsIs() {
        ChannelReceiver r = new ChannelReceiver();
        List<NetMessage> out = r.accept(List.of(
                msg(0, DeliveryMode.UNRELIABLE, 0, "a"),
                msg(0, DeliveryMode.UNRELIABLE, 2, "c"),
                msg(0, DeliveryMode.UNRELIABLE, 1, "b")));
        assertEquals(List.of("a", "c", "b"), texts(out));
    }

    @Test
    void unreliableSequencedDropsStale() {
        ChannelReceiver r = new ChannelReceiver();
        // 0, 2 delivered; 1 arrives after 2 -> stale, dropped; 3 delivered.
        List<NetMessage> out = r.accept(List.of(
                msg(0, DeliveryMode.UNRELIABLE_SEQUENCED, 0, "s0"),
                msg(0, DeliveryMode.UNRELIABLE_SEQUENCED, 2, "s2"),
                msg(0, DeliveryMode.UNRELIABLE_SEQUENCED, 1, "s1"),
                msg(0, DeliveryMode.UNRELIABLE_SEQUENCED, 3, "s3")));
        assertEquals(List.of("s0", "s2", "s3"), texts(out));
    }

    @Test
    void reliableUnorderedDeliversEachOnceInArrivalOrder() {
        ChannelReceiver r = new ChannelReceiver();
        List<NetMessage> out = r.accept(List.of(
                msg(0, DeliveryMode.RELIABLE_UNORDERED, 0, "u0"),
                msg(0, DeliveryMode.RELIABLE_UNORDERED, 2, "u2"),
                msg(0, DeliveryMode.RELIABLE_UNORDERED, 2, "u2dup"),
                msg(0, DeliveryMode.RELIABLE_UNORDERED, 1, "u1"),
                msg(0, DeliveryMode.RELIABLE_UNORDERED, 0, "u0dup")));
        assertEquals(List.of("u0", "u2", "u1"), texts(out), "each delivered once, immediately, no reorder");
    }

    @Test
    void reliableOrderedReconstructsOrderAndDropsDuplicates() {
        ChannelReceiver r = new ChannelReceiver();
        // Arrive 2, 0, 1, 3 with a duplicate 0 -> release strictly in order 0,1,2,3.
        List<NetMessage> out = r.accept(List.of(
                msg(0, DeliveryMode.RELIABLE_ORDERED, 2, "o2"),
                msg(0, DeliveryMode.RELIABLE_ORDERED, 0, "o0"),
                msg(0, DeliveryMode.RELIABLE_ORDERED, 0, "o0dup"),
                msg(0, DeliveryMode.RELIABLE_ORDERED, 1, "o1"),
                msg(0, DeliveryMode.RELIABLE_ORDERED, 3, "o3")));
        assertEquals(List.of("o0", "o1", "o2", "o3"), texts(out));
    }

    @Test
    void channelsAreIndependent() {
        ChannelReceiver r = new ChannelReceiver();
        // Channel 0 ordered is missing seq 0 (only 1 arrives -> buffered, nothing released);
        // channel 1 unreliable still delivers. No cross-channel head-of-line blocking.
        List<NetMessage> out = r.accept(List.of(
                msg(0, DeliveryMode.RELIABLE_ORDERED, 1, "blocked"),
                msg(1, DeliveryMode.UNRELIABLE, 0, "free")));
        assertEquals(List.of("free"), texts(out));
    }

    @Test
    void senderRetransmitsUnackedReliableMessagesAfterRto() {
        ChannelSender s = new ChannelSender();
        long rto = 100_000_000L; // 100ms
        s.enqueue(0, DeliveryMode.RELIABLE_ORDERED, bytes("r"));

        List<NetMessage> first = s.pack(5, 0, rto, 1200);
        assertEquals(1, first.size(), "new reliable message is sent");

        // Before the RTO elapses it is NOT resent...
        assertTrue(s.pack(6, 50_000_000L, rto, 1200).isEmpty());
        // ...after the RTO it IS resent (still unacked).
        assertEquals(1, s.pack(7, 200_000_000L, rto, 1200).size());

        // Once the carrying packet is acked, it is gone for good.
        s.acked(7);
        assertTrue(s.pack(8, 400_000_000L, rto, 1200).isEmpty());
    }

    @Test
    void senderSendsUnreliableOnlyOnce() {
        ChannelSender s = new ChannelSender();
        s.enqueue(0, DeliveryMode.UNRELIABLE, bytes("x"));
        assertEquals(1, s.pack(1, 0, 100_000_000L, 1200).size());
        assertTrue(s.pack(2, 0, 100_000_000L, 1200).isEmpty(), "unreliable is not retained");
    }
}
