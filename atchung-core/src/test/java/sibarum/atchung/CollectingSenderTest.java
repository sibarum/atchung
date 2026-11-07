package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Sender#collecting}: what a part sends, kept in a list at once, with no bus, tick or inbox for a test to build
 * (Vexplore framework-notes FN-16).
 */
class CollectingSenderTest {

    private static final Topic<String> WORDS = Topic.of("test.collecting.words", String.class);

    /** A part that takes a sender, as a job with a progress topic or a widget does. */
    private static void count(Sender<String> out, int to) {
        for (int i = 1; i <= to; i++) {
            out.send("at " + i);
        }
    }

    @Test
    void whatIsSentIsInTheListAtOnceAndInOrder() {
        List<String> sent = new ArrayList<>();
        Sender<String> out = Sender.collecting(WORDS, sent);
        count(out, 3);
        assertEquals(List.of("at 1", "at 2", "at 3"), sent);
        assertEquals(WORDS, out.topic());
        assertTrue(out.toString().contains("test.collecting.words"), out.toString());
    }

    @Test
    void aListOfAWiderTypeTakesIt() {
        List<Object> sent = new ArrayList<>();
        Sender.collecting(WORDS, sent).send("one");
        assertEquals(List.of("one"), sent);
    }

    @Test
    void nothingIsNullAndNoNullIsSent() {
        assertThrows(NullPointerException.class, () -> Sender.collecting(null, new ArrayList<String>()));
        assertThrows(NullPointerException.class, () -> Sender.collecting(WORDS, null));
        Sender<String> out = Sender.collecting(WORDS, new ArrayList<>());
        assertThrows(NullPointerException.class, () -> out.send(null), "as every sender refuses one");
    }
}
