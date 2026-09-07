package sibarum.elektro.queue.codegen;

import sibarum.elektro.queue.message.Message;
import sibarum.elektro.queue.message.WireField;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Generates a reflection-free {@link sibarum.elektro.queue.wire.Codec} and a
 * {@code MessageType} descriptor for every {@code @Message} record, plus one
 * {@code ElektroRegistrar} per compilation that registers them all into a
 * {@link sibarum.elektro.queue.message.MessageRegistry}.
 *
 * <p>The registrar is named after the messages it found &mdash; {@code ElektroRegistrar} in
 * their common package, or the name given by {@code -Aelektroq.registrar} &mdash; because it
 * can only ever know the codecs generated beside it. Two modules compiled separately would
 * otherwise both claim one fixed name, and a classpath holding both would answer with
 * whichever it met first, leaving the other module's types unregistered. See
 * {@link sibarum.elektro.queue.message.MessageRegistrar} for how the registrars compose.
 *
 * <p>Only {@code record} types are accepted: their canonical constructor and component
 * accessors give a stable, reflection-free shape to read and reconstruct. Fields are
 * laid out by {@link WireField#order()} (or declaration order when no component is
 * annotated), and trailing fields marked {@code optional} or {@code since > 1} are read
 * behind a {@code hasRemaining()} guard so a newer decoder tolerates an older, shorter
 * payload.
 */
@SupportedAnnotationTypes("sibarum.elektro.queue.message.Message")
@SupportedOptions(ElektroProcessor.REGISTRAR_OPTION)
public final class ElektroProcessor extends AbstractProcessor {

    /**
     * Processor option naming the registrar outright, e.g.
     * {@code -Aelektroq.registrar=com.example.ChatRegistrar}. Needed when a module's
     * messages share no package prefix, and useful when the derived name is not the one
     * an application wants to write at its call sites.
     */
    static final String REGISTRAR_OPTION = "elektroq.registrar";

    private static final String REGISTRAR_SIMPLE = "ElektroRegistrar";

    private final List<String> generatedCodecs = new ArrayList<>();
    private final Set<String> messagePackages = new LinkedHashSet<>();
    private boolean registrarWritten;
    private Messager messager;
    private Filer filer;
    private Elements elements;

    @Override
    public synchronized void init(ProcessingEnvironment env) {
        super.init(env);
        this.messager = env.getMessager();
        this.filer = env.getFiler();
        this.elements = env.getElementUtils();
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> messages = roundEnv.getElementsAnnotatedWith(Message.class);
        for (Element element : messages) {
            try {
                generateCodec((TypeElement) element);
            } catch (AbortType e) {
                // Diagnostic already emitted; skip this type and keep going.
            }
        }
        // Emit the aggregating registrar in the same round the messages are seen, not in
        // the processingOver round: original sources (e.g. a main class) that reference it
        // must be able to resolve it in a later round.
        if (!registrarWritten && !generatedCodecs.isEmpty()) {
            generateRegistrar();
            registrarWritten = true;
        }
        return true;
    }

    // --- model ------------------------------------------------------------------

    /** One serialized field: a record component with its resolved wire attributes. */
    private record Field(String name, TypeMirror type, int order, int since, boolean optional) {
        boolean guarded() {
            return optional || since > 1;
        }
    }

    private void generateCodec(TypeElement type) {
        if (type.getKind() != ElementKind.RECORD) {
            fail(type, "@Message may only be applied to a record");
        }
        if (elements.getPackageOf(type).isUnnamed()) {
            fail(type, "@Message types must live in a named package");
        }
        if (type.getNestingKind().isNested()) {
            fail(type, "@Message must be a top-level record, not a nested one");
        }

        Message message = type.getAnnotation(Message.class);
        List<Field> declared = collectFields(type);
        List<Field> wire = new ArrayList<>(declared);
        wire.sort(Comparator.comparingInt(Field::order));
        validateWireOrder(type, wire);

        String pkg = elements.getPackageOf(type).getQualifiedName().toString();
        String simple = type.getSimpleName().toString();
        String name = message.name().isEmpty() ? simple : message.name();
        String codecFqn = pkg + "." + simple + "Codec";

        String source = emit(pkg, simple, simple + "Codec", name, message, declared, wire);
        write(codecFqn, type, source);
        generatedCodecs.add(codecFqn);
        messagePackages.add(pkg);
    }

    private List<Field> collectFields(TypeElement type) {
        List<? extends RecordComponentElement> components = type.getRecordComponents();
        long annotated = components.stream().filter(c -> c.getAnnotation(WireField.class) != null).count();
        if (annotated != 0 && annotated != components.size()) {
            fail(type, "@WireField must annotate either every component or none");
        }
        List<Field> fields = new ArrayList<>(components.size());
        int index = 0;
        for (RecordComponentElement component : components) {
            WireField wf = component.getAnnotation(WireField.class);
            int order = wf != null ? wf.order() : index;
            int since = wf != null ? wf.since() : 1;
            boolean optional = wf != null && wf.optional();
            fields.add(new Field(component.getSimpleName().toString(), component.asType(), order, since, optional));
            index++;
        }
        return fields;
    }

    private void validateWireOrder(TypeElement type, List<Field> wire) {
        boolean seenGuarded = false;
        for (int i = 0; i < wire.size(); i++) {
            if (i > 0 && wire.get(i).order() == wire.get(i - 1).order()) {
                fail(type, "duplicate @WireField order " + wire.get(i).order());
            }
            if (wire.get(i).guarded()) {
                seenGuarded = true;
            } else if (seenGuarded) {
                fail(type, "required field '" + wire.get(i).name()
                        + "' must not follow an optional or since>1 field on the wire");
            }
        }
    }

    // --- emit -------------------------------------------------------------------

    private String emit(String pkg, String simple, String codecSimple, String name,
                        Message message, List<Field> declared, List<Field> wire) {
        StringBuilder b = new StringBuilder(1024);
        b.append("package ").append(pkg).append(";\n\n");
        b.append("// Generated by elektro-Q. Do not edit.\n");
        b.append("public final class ").append(codecSimple)
         .append(" implements sibarum.elektro.queue.wire.Codec<").append(simple).append("> {\n\n");

        b.append("    public static final ").append(codecSimple)
         .append(" INSTANCE = new ").append(codecSimple).append("();\n\n");

        b.append("    public static final sibarum.elektro.queue.message.MessageType<").append(simple)
         .append("> TYPE = new sibarum.elektro.queue.message.MessageType<>(")
         .append(message.id()).append(", ").append(message.schemaVersion()).append(", \"")
         .append(name).append("\", ").append(simple).append(".class, INSTANCE);\n\n");

        b.append("    private ").append(codecSimple).append("() {}\n\n");

        // encode
        b.append("    @Override\n");
        b.append("    public void encode(").append(simple)
         .append(" $value, sibarum.elektro.queue.wire.WireWriter $out) {\n");
        for (Field f : wire) {
            for (String line : encodeLines(f)) {
                b.append("        ").append(line).append('\n');
            }
        }
        b.append("    }\n\n");

        // decode
        b.append("    @Override\n");
        b.append("    public ").append(simple)
         .append(" decode(sibarum.elektro.queue.wire.WireReader $in) {\n");
        for (Field f : wire) {
            String typeName = f.type().toString();
            if (f.guarded()) {
                b.append("        ").append(typeName).append(' ').append(f.name())
                 .append(" = ").append(defaultLiteral(f.type())).append(";\n");
                b.append("        if ($in.hasRemaining()) { ").append(f.name())
                 .append(" = ").append(decodeExpr(f)).append("; }\n");
            } else {
                b.append("        ").append(typeName).append(' ').append(f.name())
                 .append(" = ").append(decodeExpr(f)).append(";\n");
            }
        }
        b.append("        return new ").append(simple).append('(');
        for (int i = 0; i < declared.size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(declared.get(i).name());
        }
        b.append(");\n");
        b.append("    }\n");

        b.append("}\n");
        return b.toString();
    }

    private List<String> encodeLines(Field f) {
        String accessor = "$value." + f.name() + "()";
        String op = primitiveOp(f.type());
        if (op != null) {
            return List.of("$out.put" + op + "(" + accessor + ");");
        }
        if (isByteArray(f.type())) {
            return List.of(
                    "$out.putVarInt(" + accessor + ".length);",
                    "$out.putBytes(" + accessor + ");");
        }
        String nested = nestedCodecFqn(f);
        return List.of(nested + ".INSTANCE.encode(" + accessor + ", $out);");
    }

    private String decodeExpr(Field f) {
        String op = primitiveOp(f.type());
        if (op != null) {
            return "$in.get" + op + "()";
        }
        if (isByteArray(f.type())) {
            return "$in.getBytes($in.getVarInt())";
        }
        return nestedCodecFqn(f) + ".INSTANCE.decode($in)";
    }

    /** Returns the WireWriter/WireReader op suffix for a primitive/String field, else null. */
    private String primitiveOp(TypeMirror type) {
        return switch (type.getKind()) {
            case BOOLEAN -> "Boolean";
            case BYTE -> "Byte";
            case SHORT -> "Short";
            case INT -> "Int";
            case LONG -> "Long";
            case FLOAT -> "Float";
            case DOUBLE -> "Double";
            case DECLARED -> isString(type) ? "String" : null;
            default -> null;
        };
    }

    private boolean isString(TypeMirror type) {
        return type.getKind() == TypeKind.DECLARED
                && ((TypeElement) ((DeclaredType) type).asElement())
                        .getQualifiedName().contentEquals("java.lang.String");
    }

    private boolean isByteArray(TypeMirror type) {
        return type.getKind() == TypeKind.ARRAY
                && ((ArrayType) type).getComponentType().getKind() == TypeKind.BYTE;
    }

    /** FQN of the generated codec for a nested @Message field, or fails if the type is unsupported. */
    private String nestedCodecFqn(Field f) {
        TypeMirror type = f.type();
        if (type.getKind() == TypeKind.DECLARED) {
            Element element = ((DeclaredType) type).asElement();
            if (element.getKind() == ElementKind.RECORD && element.getAnnotation(Message.class) != null) {
                TypeElement te = (TypeElement) element;
                return elements.getPackageOf(te).getQualifiedName() + "." + te.getSimpleName() + "Codec";
            }
        }
        throw fail(null, "unsupported wire type '" + type + "' for field '" + f.name()
                + "'; supported: primitives, String, byte[], and nested @Message records");
    }

    private String defaultLiteral(TypeMirror type) {
        return switch (type.getKind()) {
            case BOOLEAN -> "false";
            case BYTE, SHORT, INT -> "0";
            case LONG -> "0L";
            case FLOAT -> "0.0f";
            case DOUBLE -> "0.0d";
            default -> "null";
        };
    }

    // --- registrar --------------------------------------------------------------

    /**
     * Emits the aggregating registrar for this compilation unit, named so that two modules
     * compiled separately cannot produce the same class.
     *
     * <p>A registrar only ever knows the codecs generated alongside it, so one fixed name
     * shared by every module would be a lie on any classpath carrying two of them: the
     * loader answers with whichever it finds first, the other module's types never reach
     * the registry, and its frames are dropped on arrival as unknown ids. Deriving the name
     * from the messages themselves keeps the registrars distinct, and
     * {@code ArrayMessageRegistry.of(...)} then composes them by name at the call site.
     */
    private void generateRegistrar() {
        String fqn = registrarFqn();
        if (fqn == null) {
            return; // diagnostic already emitted
        }
        int split = fqn.lastIndexOf('.');
        String pkg = fqn.substring(0, split);
        String simple = fqn.substring(split + 1);

        StringBuilder b = new StringBuilder(512);
        b.append("package ").append(pkg).append(";\n\n");
        b.append("// Generated by elektro-Q. Do not edit.\n");
        b.append("public final class ").append(simple)
         .append(" implements sibarum.elektro.queue.message.MessageRegistrar {\n\n");

        b.append("    public static final ").append(simple)
         .append(" INSTANCE = new ").append(simple).append("();\n\n");

        b.append("    private ").append(simple).append("() {}\n\n");

        b.append("    @Override\n");
        b.append("    public void registerInto(sibarum.elektro.queue.message.MessageRegistry registry) {\n");
        for (String codec : generatedCodecs) {
            b.append("        registry.register(").append(codec).append(".TYPE);\n");
        }
        b.append("    }\n\n");

        b.append("    /** Shorthand for {@code INSTANCE.registerInto(registry)}. */\n");
        b.append("    public static void registerAll(sibarum.elektro.queue.message.MessageRegistry registry) {\n");
        b.append("        INSTANCE.registerInto(registry);\n");
        b.append("    }\n");
        b.append("}\n");
        write(fqn, null, b.toString());
    }

    /** The registrar's fully-qualified name: the {@code -A} option if given, else derived. */
    private String registrarFqn() {
        String configured = processingEnv.getOptions().get(REGISTRAR_OPTION);
        if (configured != null && !configured.isBlank()) {
            String trimmed = configured.trim();
            if (trimmed.lastIndexOf('.') < 1) {
                messager.printMessage(Diagnostic.Kind.ERROR, "-A" + REGISTRAR_OPTION
                        + " must be a fully-qualified name in a named package, was '" + trimmed + "'");
                return null;
            }
            return trimmed;
        }
        String pkg = commonPackagePrefix(messagePackages);
        if (pkg.isEmpty()) {
            messager.printMessage(Diagnostic.Kind.ERROR, "@Message types in this compilation share no"
                    + " package prefix (" + String.join(", ", messagePackages) + "), so the registrar"
                    + " cannot be named after them; pass -A" + REGISTRAR_OPTION + "=<fully.qualified.Name>");
            return null;
        }
        return pkg + "." + REGISTRAR_SIMPLE;
    }

    /**
     * The longest package all of {@code packages} sit under, matched a segment at a time so
     * {@code com.example.chat} and {@code com.example.chatter} share {@code com.example}
     * rather than the common text. Empty when they share no root.
     */
    private String commonPackagePrefix(Set<String> packages) {
        String[] prefix = null;
        for (String pkg : packages) {
            String[] segments = pkg.split("[.]");
            if (prefix == null) {
                prefix = segments;
                continue;
            }
            int shared = 0;
            while (shared < prefix.length && shared < segments.length
                    && prefix[shared].equals(segments[shared])) {
                shared++;
            }
            prefix = Arrays.copyOf(prefix, shared);
        }
        return prefix == null ? "" : String.join(".", prefix);
    }

    // --- io / diagnostics -------------------------------------------------------

    private void write(String fqn, Element origin, String source) {
        try {
            JavaFileObject file = origin != null
                    ? filer.createSourceFile(fqn, origin)
                    : filer.createSourceFile(fqn);
            try (Writer writer = file.openWriter()) {
                writer.write(source);
            }
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, "Failed to write " + fqn + ": " + e.getMessage());
        }
    }

    /** Signals an unrecoverable problem with one type; emits an error and aborts just that type. */
    private AbortType fail(Element element, String message) {
        messager.printMessage(Diagnostic.Kind.ERROR, message, element);
        throw new AbortType();
    }

    private static final class AbortType extends RuntimeException {
        AbortType() {
            super(null, null, false, false);
        }
    }
}
