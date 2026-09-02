/**
 * The stack-wide profiling seam: <em>verbose on demand</em>.
 *
 * <p>Start at {@link sibarum.probe.Probe}. Everything else here supports it.
 *
 * <p>This package has no dependencies, uses no reflection, and starts no threads unless profiling is on, so
 * any module in the stack can depend on it — including the ones that must not depend on the event bus. That
 * is why it is a module of its own rather than a corner of {@code atchung-core}.
 */
package sibarum.probe;
