package sibarum.elektro.queue.message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type as an elektro-Q message and assigns it a stable wire identity.
 *
 * <p>The elektro-Q annotation processor discovers every {@code @Message} type at
 * compile time and generates a {@link sibarum.elektro.queue.wire.Codec} plus a
 * registration entry for it. Because retention is {@link RetentionPolicy#SOURCE},
 * the annotation leaves no trace at runtime: there is no reflective lookup and
 * nothing for GraalVM native-image to configure.
 *
 * <p>The {@link #id()} is the contract between peers &mdash; it must remain stable
 * for the life of the protocol so that a message emitted by one version can be
 * routed by another. Evolve the <i>shape</i> of a message with {@link #schemaVersion()}
 * and {@link WireField#since()}, never by reusing or renumbering an {@code id}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Message {

    /** Globally stable numeric identity carried in the envelope header. */
    int id();

    /** Layout version, incremented when fields are added. Defaults to {@code 1}. */
    int schemaVersion() default 1;

    /** Optional human-readable name for diagnostics; defaults to the simple type name. */
    String name() default "";
}
