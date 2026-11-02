package sibarum.atchung;

import java.time.Duration;

/**
 * A {@link Chain.Builder#none NONE} bounded by a time saw nothing it waited not to see: the outcome it goes on with.
 *
 * @param chain the chain's name
 * @param at    the wait
 * @param after how long it was quiet
 */
public record Quiet(String chain, String at, Duration after) {
}
