package sibarum.atchung;

import java.util.List;

/**
 * An {@link Chain.Builder#all ALL} could not go on: its bound ran out, or one of its branches came back with
 * something other than what it waited for (a chain that timed out, say). The next link takes it by type.
 *
 * @param chain     the chain's name
 * @param at        the wait
 * @param missing   the branches that had not arrived as waited for
 * @param byDefault whether it was the tick's default bound that ran out (see {@link TimedOut#byDefault})
 */
public record Missing(String chain, String at, List<String> missing, boolean byDefault) {

    public Missing {
        missing = List.copyOf(missing);
    }
}
