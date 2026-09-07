package sibarum.elektro.queue.codegen;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistrar;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compiles annotated records with the real {@code javac} + {@link ElektroProcessor},
 * loads the generated codecs and registrar, and round-trips instances through them.
 */
class ProcessorIntegrationTest {

    private static final Map<String, String> SOURCES = Map.of(
            "demo.Ping", """
                package demo;
                import sibarum.elektro.queue.message.Message;
                @Message(id = 1)
                public record Ping(int seq, String name, boolean flag, double ratio) {}
                """,
            "demo.Inner", """
                package demo;
                import sibarum.elektro.queue.message.Message;
                @Message(id = 3)
                public record Inner(long v, String label) {}
                """,
            "demo.Outer", """
                package demo;
                import sibarum.elektro.queue.message.Message;
                @Message(id = 4)
                public record Outer(int x, Inner in) {}
                """,
            "demo.Blob", """
                package demo;
                import sibarum.elektro.queue.message.Message;
                @Message(id = 5)
                public record Blob(byte[] data, int tag) {}
                """,
            "demo.Versioned", """
                package demo;
                import sibarum.elektro.queue.message.Message;
                import sibarum.elektro.queue.message.WireField;
                @Message(id = 2, schemaVersion = 2)
                public record Versioned(
                        @WireField(order = 1) int a,
                        @WireField(order = 2) String b,
                        @WireField(order = 3, since = 2, optional = true) int c) {}
                """);

    private static ClassLoader generated;

    @BeforeAll
    static void compileAll() throws IOException {
        generated = loader(compileWithProcessor(SOURCES));
    }

    @Test
    void primitiveRecordRoundTrips() throws Exception {
        Class<?> ping = generated.loadClass("demo.Ping");
        Object original = ping.getConstructor(int.class, String.class, boolean.class, double.class)
                .newInstance(7, "echo", true, 3.5d);

        Object decoded = roundTrip("demo.PingCodec", original);
        assertEquals(original, decoded);
    }

    @Test
    void nestedMessageRoundTrips() throws Exception {
        Class<?> inner = generated.loadClass("demo.Inner");
        Class<?> outer = generated.loadClass("demo.Outer");
        Object innerVal = inner.getConstructor(long.class, String.class).newInstance(99L, "deep");
        Object original = outer.getConstructor(int.class, inner).newInstance(12, innerVal);

        Object decoded = roundTrip("demo.OuterCodec", original);
        assertEquals(original, decoded);
    }

    @Test
    void byteArrayFieldRoundTrips() throws Exception {
        Class<?> blob = generated.loadClass("demo.Blob");
        byte[] payload = {9, 8, 7, 0, -1};
        Object original = blob.getConstructor(byte[].class, int.class).newInstance(payload, 42);

        Object decoded = roundTrip("demo.BlobCodec", original);
        assertArrayEquals(payload, (byte[]) blob.getMethod("data").invoke(decoded));
        assertEquals(42, blob.getMethod("tag").invoke(decoded));
    }

    @Test
    void generatedTypeCarriesIdAndSchemaVersion() throws Exception {
        MessageType<?> type = typeOf("demo.VersionedCodec");
        assertEquals(2, type.id());
        assertEquals(2, type.schemaVersion());
        assertEquals("Versioned", type.name());
    }

    @Test
    void olderShorterPayloadDecodesWithDefaultForNewField() throws Exception {
        // A schema-v1 peer wrote only a and b; the since=2 field c must default to 0.
        WireBufferWriter w = new WireBufferWriter();
        w.putInt(5).putString("x");

        @SuppressWarnings("unchecked")
        Codec<Object> codec = (Codec<Object>) instance("demo.VersionedCodec");
        Object decoded = codec.decode(new WireBufferReader(w.toByteArray()));

        Class<?> versioned = generated.loadClass("demo.Versioned");
        assertEquals(5, versioned.getMethod("a").invoke(decoded));
        assertEquals("x", versioned.getMethod("b").invoke(decoded));
        assertEquals(0, versioned.getMethod("c").invoke(decoded));
    }

    @Test
    void registrarRegistersEveryMessage() throws Exception {
        MessageRegistry registry = new ArrayMessageRegistry();
        Class<?> registrar = generated.loadClass("demo.ElektroRegistrar");
        registrar.getMethod("registerAll", MessageRegistry.class).invoke(null, registry);

        assertEquals(5, registry.size());
        assertEquals("Ping", registry.byId(1).name());
        assertEquals("Versioned", registry.byId(2).name());
        assertEquals("Outer", registry.byId(4).name());
        assertTrue(registry.contains(5));
    }

    /**
     * Two modules, compiled separately as Maven compiles them, must not both produce one
     * fixed {@code ElektroRegistrar}: on a classpath carrying both, the loader would answer
     * with whichever it met first and the other module's types would never be registered.
     * Each registrar is named after its own messages, and the two compose into one registry.
     */
    @Test
    void separateCompilationsProduceDistinctRegistrarsThatCompose() throws Exception {
        Path alphaOut = compileWithProcessor(Map.of(
                "alpha.Move", """
                    package alpha;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 10)
                    public record Move(int dx, int dy) {}
                    """));
        Path betaOut = compileWithProcessor(Map.of(
                "beta.Chat", """
                    package beta;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 20)
                    public record Chat(String text) {}
                    """,
                "beta.Ack", """
                    package beta;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 21)
                    public record Ack(long seq) {}
                    """));

        // The old fixed name is gone: neither compilation can shadow the other.
        assertFalse(Files.exists(alphaOut.resolve("sibarum/elektro/queue/generated/ElektroRegistrar.class")));
        assertFalse(Files.exists(betaOut.resolve("sibarum/elektro/queue/generated/ElektroRegistrar.class")));
        assertTrue(Files.exists(alphaOut.resolve("alpha/ElektroRegistrar.class")));
        assertTrue(Files.exists(betaOut.resolve("beta/ElektroRegistrar.class")));

        ClassLoader both = loader(alphaOut, betaOut);
        MessageRegistry registry = ArrayMessageRegistry.of(
                registrarIn(both, "alpha.ElektroRegistrar"),
                registrarIn(both, "beta.ElektroRegistrar"));

        assertEquals(3, registry.size());
        assertEquals("Move", registry.byId(10).name());
        assertEquals("Chat", registry.byId(20).name());
        assertEquals("Ack", registry.byId(21).name());
    }

    /** Registering one module's registrar alone leaves the other module's ids unbound. */
    @Test
    void oneRegistrarBindsOnlyItsOwnModulesTypes() throws Exception {
        Path alphaOut = compileWithProcessor(Map.of(
                "alpha.Move", """
                    package alpha;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 10)
                    public record Move(int dx, int dy) {}
                    """));

        MessageRegistry registry = ArrayMessageRegistry.of(
                registrarIn(loader(alphaOut), "alpha.ElektroRegistrar"));

        assertTrue(registry.contains(10));
        assertFalse(registry.contains(20));
    }

    /** Messages spread across sibling packages put the registrar in the package above them. */
    @Test
    void registrarLandsInTheCommonPackageOfItsMessages() throws Exception {
        Path out = compileWithProcessor(Map.of(
                "multi.chat.Say", """
                    package multi.chat;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 30)
                    public record Say(String text) {}
                    """,
                "multi.admin.Kick", """
                    package multi.admin;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 31)
                    public record Kick(long who) {}
                    """));

        assertTrue(Files.exists(out.resolve("multi/ElektroRegistrar.class")));
        MessageRegistry registry = ArrayMessageRegistry.of(registrarIn(loader(out), "multi.ElektroRegistrar"));
        assertEquals(2, registry.size());
    }

    /** {@code -Aelektroq.registrar} overrides the derived name outright. */
    @Test
    void registrarNameCanBeSetByOption() throws Exception {
        Path out = compileWithProcessor(
                Map.of("named.Tick", """
                    package named;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 40)
                    public record Tick(long at) {}
                    """),
                List.of("-Aelektroq.registrar=named.wire.TickRegistrar"));

        assertTrue(Files.exists(out.resolve("named/wire/TickRegistrar.class")));
        assertFalse(Files.exists(out.resolve("named/ElektroRegistrar.class")));
        MessageRegistry registry = ArrayMessageRegistry.of(registrarIn(loader(out), "named.wire.TickRegistrar"));
        assertTrue(registry.contains(40));
    }

    /** Messages sharing no root cannot name a registrar, and say so at compile time. */
    @Test
    void messagesWithNoCommonRootFailWithAnActionableError() throws Exception {
        String errors = compileExpectingFailure(Map.of(
                "one.A", """
                    package one;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 50)
                    public record A(int x) {}
                    """,
                "two.B", """
                    package two;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 51)
                    public record B(int y) {}
                    """));

        assertTrue(errors.contains("share no package prefix"), errors);
        assertTrue(errors.contains("-Aelektroq.registrar"), errors);
    }

    /** Two modules that claim one id for different types are rejected as the registry is built. */
    @Test
    void composingRegistrarsThatClaimTheSameIdThrows() throws Exception {
        Path alphaOut = compileWithProcessor(Map.of(
                "alpha.Move", """
                    package alpha;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 60)
                    public record Move(int dx) {}
                    """));
        Path clashOut = compileWithProcessor(Map.of(
                "clash.Shove", """
                    package clash;
                    import sibarum.elektro.queue.message.Message;
                    @Message(id = 60)
                    public record Shove(int dx) {}
                    """));

        ClassLoader both = loader(alphaOut, clashOut);
        MessageRegistrar alpha = registrarIn(both, "alpha.ElektroRegistrar");
        MessageRegistrar clash = registrarIn(both, "clash.ElektroRegistrar");

        ElektroException e = assertThrows(ElektroException.class,
                () -> ArrayMessageRegistry.of(alpha, clash));
        assertTrue(e.getMessage().contains("already registered"), e.getMessage());
    }

    // --- helpers ----------------------------------------------------------------

    private Object roundTrip(String codecFqn, Object value) throws Exception {
        @SuppressWarnings("unchecked")
        Codec<Object> codec = (Codec<Object>) instance(codecFqn);
        WireBufferWriter w = new WireBufferWriter();
        codec.encode(value, w);
        return codec.decode(new WireBufferReader(w.toByteArray()));
    }

    private Object instance(String codecFqn) throws Exception {
        return generated.loadClass(codecFqn).getField("INSTANCE").get(null);
    }

    private MessageType<?> typeOf(String codecFqn) throws Exception {
        return (MessageType<?>) generated.loadClass(codecFqn).getField("TYPE").get(null);
    }

    /** The single registrar instance a compilation's generated {@code ElektroRegistrar} exposes. */
    private static MessageRegistrar registrarIn(ClassLoader loader, String fqn) throws Exception {
        return (MessageRegistrar) loader.loadClass(fqn).getField("INSTANCE").get(null);
    }

    /** A loader over the class output of one or more separate compilations, in order. */
    private static ClassLoader loader(Path... outDirs) throws IOException {
        URL[] urls = new URL[outDirs.length];
        for (int i = 0; i < outDirs.length; i++) {
            urls[i] = outDirs[i].toUri().toURL();
        }
        return new URLClassLoader(urls, ProcessorIntegrationTest.class.getClassLoader());
    }

    private static Path compileWithProcessor(Map<String, String> sources) throws IOException {
        return compileWithProcessor(sources, List.of());
    }

    /** Compiles {@code sources} with the real processor into a fresh output dir, which it returns. */
    private static Path compileWithProcessor(Map<String, String> sources, List<String> extraOptions)
            throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "a full JDK (not JRE) is required to run this test");

        Path srcDir = Files.createTempDirectory("elektroq-src");
        Path outDir = Files.createTempDirectory("elektroq-out");
        Path genDir = Files.createTempDirectory("elektroq-gen");

        List<Path> sourceFiles = new ArrayList<>();
        for (Map.Entry<String, String> entry : sources.entrySet()) {
            Path file = srcDir.resolve(entry.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.writeString(file, entry.getValue());
            sourceFiles.add(file);
        }

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");

        boolean ok;
        try (StandardJavaFileManager fm = compiler.getStandardFileManager(diagnostics, null, null)) {
            fm.setLocation(StandardLocation.CLASS_OUTPUT, List.of(outDir.toFile()));
            fm.setLocation(StandardLocation.SOURCE_OUTPUT, List.of(genDir.toFile()));
            Iterable<? extends JavaFileObject> units = fm.getJavaFileObjectsFromPaths(sourceFiles);
            List<String> options = new ArrayList<>(List.of(
                    "-classpath", classpath,
                    "-processorpath", classpath,
                    "-processor", "sibarum.elektro.queue.codegen.ElektroProcessor"));
            options.addAll(extraOptions);
            ok = compiler.getTask(null, fm, diagnostics, options, null, units).call();
        }

        if (!ok) {
            throw new AssertionError("Compilation with processor failed:\n" + errorsIn(diagnostics));
        }
        return outDir;
    }

    /** Compiles expecting failure, and returns the errors javac reported. */
    private static String compileExpectingFailure(Map<String, String> sources) throws IOException {
        try {
            compileWithProcessor(sources);
        } catch (AssertionError expected) {
            return expected.getMessage();
        }
        throw new AssertionError("expected compilation to fail, but it succeeded");
    }

    private static String errorsIn(DiagnosticCollector<JavaFileObject> diagnostics) {
        return diagnostics.getDiagnostics().stream()
                .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                .map(Object::toString)
                .collect(Collectors.joining("\n"));
    }
}
