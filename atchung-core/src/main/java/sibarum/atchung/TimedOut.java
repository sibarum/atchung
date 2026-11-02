package sibarum.atchung;

import java.time.Duration;

/**
 * A wait's bound ran out first: the outcome of a held step, an {@link Chain.Builder#any ANY}, or a chain used as a
 * branch, when what it waited for had not come. The next link takes it by type, as any outcome (chains.md, ruling 6).
 *
 * @param chain     the chain's name
 * @param at        the link that was waiting
 * @param after     how long it waited
 * @param byDefault whether the bound was the tick's default rather than one the chain stated: a default running out
 *                  is a bug, and the tick's {@link Tick.Overdue} policy hears of it (ruling 7)
 */
public record TimedOut(String chain, String at, Duration after, boolean byDefault) {
}
