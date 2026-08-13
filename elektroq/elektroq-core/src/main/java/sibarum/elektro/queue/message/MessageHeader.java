package sibarum.elektro.queue.message;

/**
 * The fixed-size envelope prefixing every frame on the wire.
 *
 * <p>The header carries everything a receiver needs to route a frame <i>before</i>
 * decoding its payload: which message {@link #typeId()} to look up in the
 * {@link MessageRegistry}, which {@link #schemaVersion()} produced it, request/reply
 * {@link #flags()} and {@link #correlationId()} for correlation, and the
 * {@link #payloadLength()} so the transport can read exactly the right number of bytes.
 *
 * <p>The magic and protocol version from {@link Protocol} are validated during framing
 * and are therefore not modelled as fields here. Serialization lives in
 * {@link HeaderCodec}.
 */
public record MessageHeader(int typeId, int schemaVersion, byte flags, long correlationId, int payloadLength) {

    public boolean isRequest() {
        return Flags.has(flags, Flags.REQUEST);
    }

    public boolean isReply() {
        return Flags.has(flags, Flags.REPLY);
    }
}
