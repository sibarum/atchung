package sibarum.elektro.queue.netcode.reliable;

import java.util.function.LongSupplier;

/**
 * The per-peer sequencing + acknowledgement engine — the heart of the netcode reliability layer
 * (docs/netcode-design.md, layer 2). It is transport-agnostic and holds no sockets: you hand it
 * payloads to {@link #stamp} into packets and raw datagrams to {@link #process}, and it maintains
 * the sequence/ack bookkeeping, round-trip-time, and packet-loss estimates.
 *
 * <p>The scheme (Fiedler / GameNetworkingSockets): every outbound packet gets its own sequence
 * number and carries an <em>acknowledgement</em> of the peer — the highest sequence received from
 * them plus a 32-bit bitfield of the 32 before it. From the acks that come back, this endpoint
 * learns which of <em>its</em> packets arrived (so a higher layer can retransmit the rest), samples
 * RTT from the send-to-ack delay, and estimates loss as packets age out of the ack window unacked.
 * No per-packet timeout is needed to detect loss — the bitfield reveals it within ~1 RTT.
 *
 * <p>Not thread-safe: a {@code ReliableTransport} confines one endpoint to its receive/tick path (or
 * synchronises around it). The clock is injectable so RTT is deterministically testable.
 */
public final class ReliableEndpoint {

    /** Ring-buffer capacity for sent/received sequence tracking; a power of two dividing 65536. */
    private static final int CAP = 1024;
    private static final int MASK = CAP - 1;
    private static final int SEQ_MODULO = 0x10000; // 65536
    private static final int ACK_WINDOW = 32;      // bits in the ack field
    private static final double EWMA_ALPHA = 0.1;

    private final int protocolId;
    private final LongSupplier nanoClock;

    // Send side.
    private int localSequence = 0;
    private final int[] sentSeq = new int[CAP];      // sequence stored in each slot, -1 if empty
    private final long[] sentTimeNanos = new long[CAP];
    private final boolean[] sentAcked = new boolean[CAP];
    private int retirePointer = 0;                   // next sent sequence whose fate to finalise
    private int lastPeerAck = -1;                    // highest ack we've seen from the peer

    // Receive side.
    private int remoteSequence = -1;                 // highest sequence received from the peer
    private final int[] recvSeq = new int[CAP];      // sequences received, for ack-bitfield generation

    // Metrics.
    private double rttMillisEwma = -1;               // -1 until the first sample
    private double lossEwma = 0;

    /** Notified as our sent packets are acknowledged — the channel layer uses this to retire messages. */
    public interface PacketListener {
        void onAcked(int sequence);
    }

    private PacketListener packetListener = seq -> { };

    public ReliableEndpoint(int protocolId) {
        this(protocolId, System::nanoTime);
    }

    public ReliableEndpoint(int protocolId, LongSupplier nanoClock) {
        this.protocolId = protocolId;
        this.nanoClock = nanoClock;
        java.util.Arrays.fill(sentSeq, -1);
        java.util.Arrays.fill(recvSeq, -1);
    }

    /** Wraps {@code payload} of the given {@link PacketType} into a packet, recording it for acking. */
    public byte[] stamp(int type, byte[] payload) {
        int seq = localSequence;
        int slot = seq & MASK;
        sentSeq[slot] = seq;
        sentTimeNanos[slot] = nanoClock.getAsLong();
        sentAcked[slot] = false;
        localSequence = (localSequence + 1) & (SEQ_MODULO - 1);
        return PacketCodec.encode(protocolId, type, seq, remoteSequence & 0xFFFF, ackBits(), payload);
    }

    /**
     * Parses an inbound datagram, updates ack/RTT/loss state, and returns the decoded packet, or
     * {@code null} if it is not one of ours (bad length or protocol id).
     */
    public Packet process(byte[] datagram) {
        Packet packet = PacketCodec.decode(datagram, protocolId);
        if (packet == null) {
            return null;
        }
        // Record that we received this sequence (for our own outgoing ack bitfield).
        int seq = packet.sequence();
        if (remoteSequence < 0 || SequenceMath.moreRecent(seq, remoteSequence)) {
            remoteSequence = seq;
        }
        recvSeq[seq & MASK] = seq;

        // Absorb the peer's acknowledgement of our packets.
        applyAck(packet.ack(), packet.ackBits());
        return packet;
    }

    /** Installs the listener notified when our sent packets are acknowledged. */
    public void setPacketListener(PacketListener listener) {
        this.packetListener = listener != null ? listener : seq -> { };
    }

    /** The sequence the next {@link #stamp} will assign (without consuming it). */
    public int peekNextSequence() {
        return localSequence;
    }

    /** Smoothed round-trip time in milliseconds, or 0 before the first sample. */
    public int rttMillis() {
        return rttMillisEwma < 0 ? 0 : (int) Math.round(rttMillisEwma);
    }

    /** Smoothed packet-loss fraction in [0,1] (packets that aged out of the ack window unacked). */
    public double packetLoss() {
        return lossEwma;
    }

    // --- internals --------------------------------------------------------------

    /** Builds the 32-bit bitfield: bit i set iff we received sequence {@code remoteSequence-(i+1)}. */
    private int ackBits() {
        if (remoteSequence < 0) {
            return 0;
        }
        int bits = 0;
        for (int i = 0; i < ACK_WINDOW; i++) {
            int seq = (remoteSequence - (i + 1)) & 0xFFFF;
            if (recvSeq[seq & MASK] == seq) {
                bits |= (1 << i);
            }
        }
        return bits;
    }

    private void applyAck(int ack, int ackBits) {
        if (lastPeerAck < 0 || SequenceMath.moreRecent(ack, lastPeerAck)) {
            lastPeerAck = ack;
        }
        ackPacket(ack);
        for (int i = 0; i < ACK_WINDOW; i++) {
            if ((ackBits & (1 << i)) != 0) {
                ackPacket((ack - (i + 1)) & 0xFFFF);
            }
        }
        retireDecided();
    }

    /** Marks one of our sent packets acknowledged (first time only) and samples RTT. */
    private void ackPacket(int seq) {
        int slot = seq & MASK;
        if (sentSeq[slot] == seq && !sentAcked[slot]) {
            sentAcked[slot] = true;
            double sampleMillis = (nanoClock.getAsLong() - sentTimeNanos[slot]) / 1_000_000.0;
            rttMillisEwma = rttMillisEwma < 0 ? sampleMillis : ewma(rttMillisEwma, sampleMillis);
            packetListener.onAcked(seq);
        }
    }

    /**
     * Finalises the fate of sent packets now provably outside the peer's ack window: acked ones feed
     * a 0 into the loss EWMA, un-acked ones (aged out) feed a 1. Advances in sequence order.
     */
    private void retireDecided() {
        if (lastPeerAck < 0) {
            return;
        }
        int horizon = (lastPeerAck - ACK_WINDOW) & 0xFFFF; // older than this is decided
        int guard = 0;
        while (retirePointer != localSequence && guard++ < CAP) {
            int slot = retirePointer & MASK;
            if (sentSeq[slot] != retirePointer) {
                retirePointer = (retirePointer + 1) & 0xFFFF; // never sent or already retired
                continue;
            }
            boolean acked = sentAcked[slot];
            boolean agedOut = SequenceMath.moreRecent(horizon, retirePointer);
            if (acked) {
                lossEwma = ewma(lossEwma, 0.0);
            } else if (agedOut) {
                lossEwma = ewma(lossEwma, 1.0);
            } else {
                break; // not yet decided; stop — later packets aren't decided either
            }
            sentSeq[slot] = -1;
            retirePointer = (retirePointer + 1) & 0xFFFF;
        }
    }

    private static double ewma(double current, double sample) {
        return current + EWMA_ALPHA * (sample - current);
    }
}
