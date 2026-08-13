/**
 * elektro-Q core API &mdash; a reflection-free message-passing library for fast
 * cross-process and networked communication, built to run under GraalVM native-image
 * and to slot in as an add-on for Truffle-hosted DSLs.
 *
 * <h2>The triad</h2>
 * <ul>
 *   <li><b>Actions emit.</b> An {@link sibarum.elektro.queue.Action} is a typed emitter
 *       bound to a {@link sibarum.elektro.queue.message.MessageType}; it encodes and
 *       sends messages fire-and-forget or as a correlated request.</li>
 *   <li><b>Conduits manage.</b> A {@link sibarum.elektro.queue.Conduit} owns a transport
 *       and its pool of peer connections, hands out actions, and routes inbound frames
 *       to actors.</li>
 *   <li><b>Actors react.</b> An {@link sibarum.elektro.queue.Actor} is registered per
 *       message type and is invoked with the decoded message and a
 *       {@link sibarum.elektro.queue.MessageContext} describing its origin.</li>
 * </ul>
 *
 * <h2>Design constraints</h2>
 * <ul>
 *   <li><b>No reflection.</b> Message identity, dispatch, and serialization run on
 *       generated code and integer ids &mdash; never on runtime reflection &mdash; so
 *       native-image needs no extra configuration.</li>
 *   <li><b>JDK-only.</b> The core depends on nothing beyond the standard library.</li>
 *   <li><b>Transport-agnostic.</b> Everything transport-specific lives behind
 *       {@link sibarum.elektro.queue.transport.Transport}; the core moves opaque framed
 *       buffers, leaving room to grow from IPC toward a full netcode / P2P stack.</li>
 *   <li><b>Versioned by construction.</b> Stable message ids plus per-type schema
 *       versions ({@link sibarum.elektro.queue.message.Message},
 *       {@link sibarum.elektro.queue.message.WireField}) let peers on different
 *       versions interoperate.</li>
 * </ul>
 *
 * <h2>Serialization</h2>
 * Messages are annotated with {@link sibarum.elektro.queue.message.Message} and their
 * fields with {@link sibarum.elektro.queue.message.WireField}. A compile-time annotation
 * processor (the planned {@code elektroq-codegen} module) generates a
 * {@link sibarum.elektro.queue.wire.Codec} and a registry entry for each, targeting the
 * {@link sibarum.elektro.queue.wire.WireWriter} / {@link sibarum.elektro.queue.wire.WireReader}
 * contract. The frame {@link sibarum.elektro.queue.message.HeaderCodec} is the worked
 * example of that layout.
 */
package sibarum.elektro.queue;
