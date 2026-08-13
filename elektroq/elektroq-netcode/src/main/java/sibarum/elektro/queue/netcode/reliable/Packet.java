package sibarum.elektro.queue.netcode.reliable;

/**
 * A decoded netcode packet: its header plus the application payload that followed (empty for control
 * packets like keepalives).
 */
public record Packet(int type, int sequence, int ack, int ackBits, byte[] payload) {
}
