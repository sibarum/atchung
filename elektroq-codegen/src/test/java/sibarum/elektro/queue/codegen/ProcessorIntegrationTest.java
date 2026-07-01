package sibarum.elektro.queue.codegen;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        generated = compileWithProcessor(SOURCES);
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
        Class<?> registrar = generated.loadClass("sibarum.elektro.queue.generated.ElektroRegistrar");
        registrar.getMethod("registerAll", MessageRegistry.class).invoke(null, registry);

        assertEquals(5, registry.size());
        assertEquals("Ping", registry.byId(1).name());
        assertEquals("Versioned", registry.byId(2).name());
        assertEquals("Outer", registry.byId(4).name());
        assertTrue(registry.contains(5));
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

    private static ClassLoader compileWithProcessor(Map<String, String> sources) throws IOException {
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
            List<String> options = List.of(
                    "-classpath", classpath,
                    "-processorpath", classpath,
                    "-processor", "sibarum.elektro.queue.codegen.ElektroProcessor");
            ok = compiler.getTask(null, fm, diagnostics, options, null, units).call();
        }

        if (!ok) {
            String report = diagnostics.getDiagnostics().stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                    .map(Object::toString)
                    .collect(Collectors.joining("\n"));
            throw new AssertionError("Compilation with processor failed:\n" + report);
        }

        return new URLClassLoader(new URL[]{outDir.toUri().toURL()},
                ProcessorIntegrationTest.class.getClassLoader());
    }
}
