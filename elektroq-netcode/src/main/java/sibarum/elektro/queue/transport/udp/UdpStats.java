package sibarum.elektro.queue.transport.udp;

/**
 * A snapshot of a {@link UdpTransport}'s raw traffic counters.
 *
 * <p>These are layer-0 metrics: how many datagrams and bytes crossed the socket. Higher-level
 * figures that need the sequencing layer &mdash; round-trip time, packet loss, retransmit rate
 * &mdash; arrive with that layer and are reported separately.
 */
public record UdpStats(long packetsSent, long bytesSent, long packetsReceived, long bytesReceived) {
}
