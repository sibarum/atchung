package sibarum.elektro.queue.netcode.reliable;

import org.junit.jupiter.api.Test;

import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic unit tests for the sequence/ack engine — no sockets, an injected clock, so RTT and
 * the ack bitfield are exact rather than timing-dependent.
 */
class ReliableEndpointTest {

    private static final int PROTO = 0xE1E2E3E4;
    private static final byte[] EMPTY = new byte[0];

    @Test
    void sequenceComparisonWrapsCorrectly() {
        assertTrue(SequenceMath.moreRecent(0, 65535), "0 is newer than 65535 across the wrap");
        assertFalse(SequenceMath.moreRecent(65535, 0));
        assertTrue(SequenceMath.moreRecent(200, 100));
        assertFalse(SequenceMath.moreRecent(100, 200));
    }

    @Test
    void ackBitfieldReflectsReceivedSequences() {
        ReliableEndpoint sender = new ReliableEndpoint(PROTO);
        ReliableEndpoint receiver = new ReliableEndpoint(PROTO);

        byte[] s0 = sender.stamp(PacketType.DATA, EMPTY); // seq 0
        byte[] s1 = sender.stamp(PacketType.DATA, EMPTY); // seq 1
        sender.stamp(PacketType.DATA, EMPTY);             // seq 2 — never delivered
        byte[] s3 = sender.stamp(PacketType.DATA, EMPTY); // seq 3

        receiver.process(s0);
        receiver.process(s1);
        receiver.process(s3); // out of order, seq 2 skipped

        Packet ack = PacketCodec.decode(receiver.stamp(PacketType.DATA, EMPTY), PROTO);
        assertEquals(3, ack.ack(), "latest received sequence");
        assertFalse((ack.ackBits() & (1 << 0)) != 0, "seq 2 (ack-1) was NOT received");
        assertTrue((ack.ackBits() & (1 << 1)) != 0, "seq 1 (ack-2) was received");
        assertTrue((ack.ackBits() & (1 << 2)) != 0, "seq 0 (ack-3) was received");
    }

    @Test
    void rttSampledFromSendToAckDelay() {
        long[] clock = {0};
        LongSupplier c = () -> clock[0];
        ReliableEndpoint a = new ReliableEndpoint(PROTO, c);
        ReliableEndpoint b = new ReliableEndpoint(PROTO, c);

        clock[0] = 0;
        byte[] p0 = a.stamp(PacketType.DATA, EMPTY);   // a's seq 0 sent at t=0
        b.process(p0);
        clock[0] = 50_000_000L;                        // t=50ms
        byte[] back = b.stamp(PacketType.DATA, EMPTY); // b's packet acks a's seq 0
        clock[0] = 100_000_000L;                       // t=100ms — a sees the ack
        a.process(back);

        assertEquals(100, a.rttMillis(), "RTT = send-to-ack round trip");
    }

    @Test
    void perfectDeliveryKeepsLossZero() {
        long[] clock = {0};
        ReliableEndpoint a = new ReliableEndpoint(PROTO, () -> clock[0]);
        ReliableEndpoint b = new ReliableEndpoint(PROTO, () -> clock[0]);

        for (int i = 0; i < 60; i++) {
            clock[0] = i * 1_000_000L;
            byte[] p = a.stamp(PacketType.DATA, EMPTY);
            b.process(p);
            a.process(b.stamp(PacketType.DATA, EMPTY)); // b acks each immediately
        }
        assertEquals(0.0, a.packetLoss(), 1e-9, "no packet was lost");
    }

    @Test
    void lossRisesWhenPacketsAgeOutUnacked() {
        long[] clock = {0};
        ReliableEndpoint a = new ReliableEndpoint(PROTO, () -> clock[0]);
        ReliableEndpoint b = new ReliableEndpoint(PROTO, () -> clock[0]);

        for (int i = 0; i < 40; i++) {
            clock[0] = i * 1_000_000L;
            byte[] p = a.stamp(PacketType.DATA, EMPTY);
            if (i % 2 == 0) {
                b.process(p); // only even sequences reach b
            }
        }
        clock[0] = 100_000_000L;
        a.process(b.stamp(PacketType.DATA, EMPTY)); // one ack back; odd sequences never acked

        assertTrue(a.packetLoss() > 0.0, "packets that aged out unacked should raise loss");
    }
}
