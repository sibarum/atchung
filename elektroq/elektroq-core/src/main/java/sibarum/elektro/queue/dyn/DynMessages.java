package sibarum.elektro.queue.dyn;

import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;

/**
 * Wiring for carrying {@link DynValue}s over a conduit.
 *
 * <p>Every dynamic value shares one {@link MessageType} &mdash; {@link #DYN} &mdash; so a single
 * integer wire id covers an unbounded set of logical types; the logical identity travels inside the
 * payload ({@link DynValue.Struct#typeName()}). Register {@link #DYN} into both peers' registries,
 * then emit with {@code conduit.action(DynMessages.DYN)} and react with
 * {@code conduit.subscribe(DynMessages.DYN, actor)}, switching on the struct's type name.
 */
public final class DynMessages {

    /**
     * Reserved wire id for the dynamic envelope. Sits far above any hand-assigned {@code @Message}
     * id so it never collides with an application's fixed-schema types.
     */
    public static final int DYN_ID = 1_000_000;

    /** The single {@link MessageType} under which all {@link DynValue}s travel. */
    public static final MessageType<DynValue> DYN =
            new MessageType<>(DYN_ID, 1, "DynValue", DynValue.class, DynCodec.INSTANCE);

    private DynMessages() {}

    /** Registers {@link #DYN} into {@code registry} (idempotent). */
    public static void registerInto(MessageRegistry registry) {
        registry.register(DYN);
    }
}
