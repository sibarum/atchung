package sibarum.elektro.queue.message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares how a field or record component of a {@link Message} type is serialized.
 *
 * <p>Fields are encoded in ascending {@link #order()} so that the wire layout is
 * independent of source declaration order. Compatibility across schema versions is
 * expressed with {@link #since()} and {@link #optional()}: a decoder generated for
 * an older schema simply stops reading once the payload is exhausted, and an
 * {@code optional} field absent from an older payload decodes to its type's default.
 *
 * <p>Retention is {@link RetentionPolicy#SOURCE}; the annotation is consumed by the
 * codec generator and never exists at runtime.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
public @interface WireField {

    /** Position of this field in the wire layout. Must be unique within a message. */
    int order();

    /** Schema version in which this field was introduced. Defaults to {@code 1}. */
    int since() default 1;

    /** If {@code true}, the field may be absent in payloads from older schemas. */
    boolean optional() default false;
}
