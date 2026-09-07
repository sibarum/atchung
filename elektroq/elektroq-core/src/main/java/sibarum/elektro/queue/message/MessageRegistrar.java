package sibarum.elektro.queue.message;

/**
 * A bundle of {@link Message} types that knows how to register itself into a
 * {@link MessageRegistry}.
 *
 * <p>The codec generator emits one implementation per compilation unit &mdash; per Maven
 * module, in practice &mdash; named {@code ElektroRegistrar} in the common package of the
 * {@code @Message} records it found. Two modules therefore produce two differently-named
 * registrars, which is the point: a single fixed name would put two classes with the same
 * fully-qualified name on one classpath, and the loader would answer with whichever it met
 * first. The other module's types would then be missing from the registry, and its frames
 * would be dropped on arrival as unknown ids &mdash; a silent, classpath-order-dependent
 * failure with nothing to read.
 *
 * <p>So aggregation is explicit instead. An application names the registrars it wants and
 * composes them:
 *
 * <pre>{@code
 * MessageRegistry registry = ArrayMessageRegistry.of(
 *         com.example.chat.ElektroRegistrar.INSTANCE,
 *         com.example.debug.ElektroRegistrar.INSTANCE);
 * }</pre>
 *
 * <p>Composition is safe and loud: {@link MessageRegistry#register} accepts an identical
 * descriptor twice but throws when two modules claim the same id for different types, so a
 * genuine id clash between modules surfaces at startup rather than on the wire.
 *
 * <p>Nothing here scans, and nothing is looked up by name: an implementation holds direct
 * references to the codecs it registers, which is what keeps a native image free of
 * reachability metadata.
 */
public interface MessageRegistrar {

    /** Registers every message type in this bundle into {@code registry}. */
    void registerInto(MessageRegistry registry);
}
