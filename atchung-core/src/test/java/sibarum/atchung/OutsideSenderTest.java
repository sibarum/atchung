package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import sibarum.probe.Cause;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** {@link Sender#outside}: every message it sends arrives in a cause of its own, under the source that sent it. */
class OutsideSenderTest {

    private static final Topic<String> EDGES = Topic.of("test.outside.edges", String.class);

    @Test
    void eachSendBeginsACauseOfItsSource() {
        Cause.Source source = Cause.source("test.outside.source", Cause.Kind.IO);
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Long> causes = new CopyOnWriteArrayList<>();
        tick.inbox("c").subscribe(EDGES, s -> causes.add(Cause.current()), 4);
        Sender<String> sender = Sender.outside(bus, EDGES, source);

        sender.send("a");
        sender.send("b");
        tick.run(System.nanoTime() + 1_000_000, Tick.ONE_THREAD);

        assertEquals(2, causes.size());
        for (long c : causes) {
            assertEquals(source.index(), Cause.sourceOf(c), "each message carries the source's cause");
        }
        assertNotEquals(causes.get(0), causes.get(1), "and a cause of its own");
    }
}
