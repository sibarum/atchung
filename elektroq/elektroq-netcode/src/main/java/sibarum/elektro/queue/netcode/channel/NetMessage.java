package sibarum.elektro.queue.netcode.channel;

/**
 * One application message as it travels inside a packet: which channel it belongs to, its delivery
 * mode, its per-channel sequence number, and the payload bytes. Several of these are packed into a
 * single DATA packet's payload (docs/netcode-design.md).
 *
 * <p>The sequence is a 32-bit per-channel counter (not the 16-bit packet sequence), so channel
 * ordering and dedup never have to reason about wraparound for any realistic session.
 */
public record NetMessage(int channelId, DeliveryMode mode, int sequence, byte[] payload) {
}
