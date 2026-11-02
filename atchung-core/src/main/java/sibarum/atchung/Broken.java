package sibarum.atchung;

/**
 * A {@link Chain.Builder#none NONE} saw what it waited not to see, before its bound: the outcome its wait fails with.
 * A link may take it; when none does, the run ends there, which is what a debounce wants.
 *
 * @param chain the chain's name
 * @param at    the wait
 * @param by    the message that broke it
 */
public record Broken(String chain, String at, Object by) {
}
