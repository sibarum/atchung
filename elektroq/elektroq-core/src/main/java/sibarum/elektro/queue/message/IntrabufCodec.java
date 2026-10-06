package sibarum.elektro.queue.message;

import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;
import sibarum.intrabuf.Header;
import sibarum.intrabuf.IbReader;
import sibarum.intrabuf.IbWriter;
import sibarum.intrabuf.MessageCodec;

/**
 * An intrabuf codec in elektro-Q's {@link Codec} shape, as {@link MessageType#of} builds it.
 *
 * <p>The conduit does not go through {@link #encode}/{@link #decode}: it recognises this codec and hands intrabuf
 * the envelope's facts directly, so a payload from another version of the schema is checked and decoded by
 * intrabuf's rules. These two methods exist for code that drives a {@code Codec} by hand; {@link #decode} reads the
 * rest of {@code in} as a body written by this very version of the schema.
 *
 * @param messageCodec the generated intrabuf codec, e.g. {@code TelemetryIntrabuf.CODEC}
 */
public record IntrabufCodec<T>(MessageCodec<T> messageCodec) implements Codec<T> {

    @Override
    public void encode(T value, WireWriter out) {
        IbWriter w = new IbWriter();
        messageCodec.encodeBody(value, w);
        out.putBytes(w.array(), 0, w.position());
    }

    @Override
    public T decode(WireReader in) {
        byte[] body = in.getBytes(in.remaining());
        return messageCodec.decodeBody(Header.of(messageCodec.schema()), new IbReader(body));
    }
}
