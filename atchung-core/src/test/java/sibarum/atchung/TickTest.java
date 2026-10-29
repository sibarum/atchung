package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Tick}: components' inboxes drained in rounds, in one order, until empty or until a deadline.
 *
 * <p>Each test is one sentence of {@code Tick}'s javadoc, and of vexelray-framework's {@code docs/threading.md} §5,
 * made to fail if it stops being true. {@link Fatal#THROW} is installed for the faults, as {@code FailFastTest} does.
 */
class TickTest {

    private static final Topic<String> A = Topic.of("test.a", String.class);
    private static final Topic<String> B = Topic.of("test.b", String.class);
    private static final Topic<String> C = Topic.of("test.c", String.class);

    record Title(int id, String text) {
    }

    private static final Topic<Title> TITLE = Topic.state("test.title", Title.class, Title::id);

    /** No deadline worth the name: every test that is not about the deadline runs a tick to the end. */
    private static final long LATER = Long.MAX_VALUE;

    private Fatal previous;

    @BeforeEach
    void substituteTheFatalPolicy() {
        previous = Atchung.fatal();
        Atchung.onFatal(Fatal.THROW);
    }

    @AfterEach
    void restoreIt() {
        Atchung.onFatal(previous);
    }

    private static long later() {
        return System.nanoTime() + 60_000_000_000L;
    }

    // ------------------------------------------------------------------------------------------- arrival

    @Test
    void aMessageFromOutsideWaitsForTheNextTickAndWakesWhoeverRunsIt() {
        Atchung bus = Atchung.create();
        AtomicInteger woken = new AtomicInteger();
        Tick tick = bus.tick(woken::incrementAndGet);
        List<String> seen = new ArrayList<>();
        tick.inbox("c").subscribe(A, seen::add, 16);

        bus.publish(A, "x");

        assertEquals(List.of(), seen, "nothing is delivered on the publisher's thread");
        assertEquals(1, woken.get(), "the arrival hook ran, so a frame loop would wake");
        assertTrue(tick.hasPending());
        assertEquals(1, tick.run(later()));
        assertEquals(List.of("x"), seen);
        assertFalse(tick.hasPending());
    }

    @Test
    void anIdleTickDeliversNothingAndTakesNoRound() {
        Tick tick = Atchung.create().tick(null);
        tick.inbox("c").subscribe(A, s -> { }, 16);

        assertEquals(0, tick.run(later()));
        assertEquals(0, tick.lastRounds(), "a tick with nothing queued is skipped, never polled (T5.3)");
    }

    @Test
    void oneInboxReceivesEveryTopicInTheOrderItWasPublished() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox c = tick.inbox("c");
        c.subscribe(A, s -> seen.add("a:" + s), 16);
        c.subscribe(B, s -> seen.add("b:" + s), 16);

        bus.publish(B, "1");
        bus.publish(A, "2");
        bus.publish(B, "3");
        tick.run(later());

        assertEquals(List.of("b:1", "a:2", "b:3"), seen,
                "one queue per component, not per topic: the two mailboxes are in order with each other");
    }

    // ------------------------------------------------------------------------------------------- rounds

    @Test
    void aReplyIsDeliveredInTheSameTickInTheNextRound() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox editor = tick.inbox("editor");
        Inbox view = tick.inbox("view");
        Sender<String> toView = editor.sender(B);
        editor.subscribe(A, s -> toView.send("shown " + s), 16);
        view.subscribe(B, seen::add, 16);

        bus.publish(A, "x");
        assertEquals(2, tick.run(later()));

        assertEquals(List.of("shown x"), seen, "a chain settles inside one tick (T5.5)");
        assertEquals(2, tick.lastRounds());
    }

    @Test
    void aSendWaitsForTheEndOfItsRoundEvenForAnInboxNotYetDelivered() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox first = tick.inbox("first");
        Inbox second = tick.inbox("second");
        Sender<String> toSecond = first.sender(B);
        first.subscribe(A, s -> toSecond.send("from first"), 16);
        second.subscribe(B, seen::add, 16);
        second.subscribe(A, s -> seen.add("outside " + s), 16);

        bus.publish(A, "x");
        tick.run(later());

        assertEquals(List.of("outside x", "from first"), seen,
                "what a round sends is the next round's, so running the inboxes one after another and running "
                        + "them at once give the same deliveries");
    }

    @Test
    void sendsOfOneRoundArriveInInboxOrderHoweverTheLanesRan() throws Exception {
        for (Tick.Lanes lanes : List.of(Tick.ONE_THREAD, backwards(), pool())) {
            Atchung bus = Atchung.create();
            Tick tick = bus.tick(null);
            List<String> seen = new ArrayList<>();
            Inbox one = tick.inbox("one", 0);
            Inbox two = tick.inbox("two", 1);
            Inbox three = tick.inbox("three", 2);
            Sender<String> fromOne = one.sender(B);
            Sender<String> fromTwo = two.sender(B);
            one.subscribe(A, s -> {
                fromOne.send("one-1");
                fromOne.send("one-2");
            }, 16);
            two.subscribe(A, s -> fromTwo.send("two-1"), 16);
            three.subscribe(B, seen::add, 16);

            bus.publish(A, "go");
            tick.run(later(), lanes);

            assertEquals(List.of("one-1", "one-2", "two-1"), seen,
                    "a round's sends are ordered by the sender's place in the wiring, then by its own order (T5.6)");
        }
    }

    /** Lanes run last to first: the opposite of the inbox order, so an order that came from timing would show. */
    private static Tick.Lanes backwards() {
        return (count, work) -> {
            for (int i = count - 1; i >= 0; i--) {
                work.lane(i);
            }
        };
    }

    /** Lanes run on a pool, really at once. */
    private static Tick.Lanes pool() {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        return (count, work) -> {
            List<Future<?>> running = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int lane = i;
                running.add(pool.submit(() -> work.lane(lane)));
            }
            for (Future<?> f : running) {
                try {
                    f.get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    // ------------------------------------------------------------------------------------------- state and edges

    @Test
    void theLastWritePerKeyWinsAndEveryEdgeIsDelivered() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox view = tick.inbox("view");
        view.subscribe(TITLE, t -> seen.add(t.id() + "=" + t.text()), 1);
        view.subscribe(A, s -> seen.add("edge " + s), 16);

        bus.publish(TITLE, new Title(1, "a"));
        bus.publish(TITLE, new Title(2, "b"));
        bus.publish(A, "x");
        bus.publish(A, "y");
        bus.publish(TITLE, new Title(1, "c"));
        tick.run(later());

        assertEquals(List.of("2=b", "edge x", "edge y", "1=c"), seen,
                "key 1's write survived and moved behind the edges that arrived after the write it replaced (T4.1)");
    }

    @Test
    void aStateSubscriptionCannotOverflowHoweverOftenOneKeyIsWritten() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Title> seen = new ArrayList<>();
        tick.inbox("view").subscribe(TITLE, seen::add, 1);

        for (int i = 0; i < 10_000; i++) {
            bus.publish(TITLE, new Title(7, "v" + i));
        }
        tick.run(later());

        assertEquals(List.of(new Title(7, "v9999")), seen);
    }

    @Test
    void aKeyFilterDeliversOnlyTheKeysItNames() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Title> seen = new ArrayList<>();
        tick.inbox("tab 2").subscribe(TITLE, seen::add, 1, Set.of(2));
        Inbox edges = tick.inbox("edge filter");
        assertThrows(IllegalArgumentException.class, () -> edges.subscribe(A, s -> { }, 1, Set.of("x")),
                "only a state topic has keys");

        bus.publish(TITLE, new Title(1, "a"));
        bus.publish(TITLE, new Title(2, "b"));
        tick.run(later());

        assertEquals(List.of(new Title(2, "b")), seen, "T4.8");
    }

    @Test
    void anEdgeMailboxThatOverflowsIsAFaultNamingTheTopic() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        tick.inbox("slow").subscribe(A, s -> { }, 2);

        bus.publish(A, "1");
        bus.publish(A, "2");
        bus.publish(A, "3");
        MailboxOverflow overflow = assertThrows(MailboxOverflow.class, () -> tick.run(later()));

        assertEquals(A, overflow.topic(), "an edge is never lost quietly (T4.2)");
        assertEquals(2, overflow.capacity());
    }

    @Test
    void twoDeclarationsOfOneChannelMustAgreeAboutItsKind() {
        Atchung bus = Atchung.create();
        Topic<Title> asEdge = Topic.of("test.title", Title.class);
        assertEquals(TITLE, asEdge, "the kind is not part of equality, so the bus sees both declarations");

        Tick tick = bus.tick(null);
        tick.inbox("one").subscribe(TITLE, t -> { }, 1);
        Inbox two = tick.inbox("two");
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> two.subscribe(asEdge, t -> { }, 1));
        assertTrue(refused.getMessage().contains("test.title"));

        bus.subscribe(asEdge, t -> { });
        bus.pump().subscribe(asEdge, t -> { }, 4);
        assertEquals(3, bus.subscriberCount(TITLE),
                "a subscriber that is not an inbox chooses its own policy, as it always has, and is never refused");
    }

    /** A record nested here, so its canonical name has an enclosing type in it. */
    record Renamed(String to) {
    }

    /**
     * A topic named by its payload is named by the payload's canonical name, so two declarations of it anywhere are one
     * channel, and a state one folds as one declared with a string does.
     */
    @Test
    void aTopicNamedByItsPayloadIsThePayloadsCanonicalName() {
        Topic<Renamed> edge = Topic.edge(Renamed.class);
        assertEquals("sibarum.atchung.TickTest.Renamed", edge.name());
        assertEquals(Topic.Kind.EDGE, edge.kind());
        assertEquals(Topic.edge("sibarum.atchung.TickTest.Renamed", Renamed.class), edge);

        Topic<Renamed> state = Topic.state(Renamed.class, Renamed::to);
        assertEquals(edge, state, "the kind is not part of equality");
        assertEquals(Topic.Kind.STATE, state.kind());

        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> heard = new ArrayList<>();
        tick.inbox("one").subscribe(state, r -> heard.add(r.to()), 1);
        bus.publish(state, new Renamed("a"));
        bus.publish(state, new Renamed("a"));
        bus.publish(state, new Renamed("b"));
        tick.run(System.nanoTime() + 1_000_000_000L);
        assertEquals(List.of("a", "b"), heard, "the last write per key, as any state topic");
        assertEquals(Topic.Kind.STATE, Topic.state(Renamed.class).kind());
    }

    // ------------------------------------------------------------------------------------------- the deadline

    @Test
    void aDeadlineStopsBetweenRoundsAndTheRestComesFirstNextTick() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox editor = tick.inbox("editor");
        Inbox view = tick.inbox("view");
        Sender<String> toView = editor.sender(B);
        editor.subscribe(A, s -> toView.send("reply to " + s), 16);
        view.subscribe(B, seen::add, 16);

        bus.publish(A, "first");
        long past = System.nanoTime() - 1;
        assertEquals(1, tick.run(past), "the first round runs whatever the deadline, so a tick always progresses");
        assertTrue(tick.carried());
        assertEquals(List.of(), seen, "the reply waits: the deadline fell between the rounds");

        bus.publish(B, "newer");
        tick.run(later());

        assertEquals(List.of("reply to first", "newer"), seen,
                "what was carried over is delivered before what arrived since, in its original order (T5.5)");
    }

    @Test
    void aStateMessageCarriedOverIsStillReplacedByANewerWrite() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<Title> seen = new ArrayList<>();
        Inbox editor = tick.inbox("editor");
        Inbox view = tick.inbox("view");
        Sender<Title> retitle = editor.sender(TITLE);
        editor.subscribe(A, s -> retitle.send(new Title(1, s)), 16);
        view.subscribe(TITLE, seen::add, 1);

        bus.publish(A, "old");
        tick.run(System.nanoTime() - 1);
        bus.publish(TITLE, new Title(1, "new"));
        tick.run(later());

        assertEquals(List.of(new Title(1, "new")), seen, "the last write within the tick wins, carried or not");
    }

    // ------------------------------------------------------------------------------------------- loops

    @Test
    void twoComponentsAnsweringEachOtherForeverAreALoop() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null, 8);
        Inbox ping = tick.inbox("ping");
        Inbox pong = tick.inbox("pong");
        Sender<String> toPong = ping.sender(B);
        Sender<String> toPing = pong.sender(A);
        ping.subscribe(A, toPong::send, 16);
        pong.subscribe(B, toPing::send, 16);

        bus.publish(A, "serve");
        TickLoop loop = assertThrows(TickLoop.class, () -> tick.run(later()));

        assertEquals(9, loop.hops(), "T5.7");
    }

    @Test
    void aLoopIsCaughtEvenWhenEveryTickIsCutShortByItsDeadline() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null, 8);
        Inbox ping = tick.inbox("ping");
        Inbox pong = tick.inbox("pong");
        Sender<String> toPong = ping.sender(B);
        Sender<String> toPing = pong.sender(A);
        ping.subscribe(A, toPong::send, 16);
        pong.subscribe(B, toPing::send, 16);

        bus.publish(A, "serve");
        assertThrows(TickLoop.class, () -> {
            for (int i = 0; i < 100; i++) {
                tick.run(System.nanoTime() - 1);
            }
        }, "hops are counted per message, not rounds per tick, so a deadline cannot hide a loop");
    }

    // ------------------------------------------------------------------------------------------- failure

    @Test
    void aThrowIsRethrownAfterTheRoundAndTheRestStaysQueued() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Pump outside = bus.pump();
        List<String> out = new ArrayList<>();
        outside.subscribe(C, out::add, 16);
        Inbox c = tick.inbox("c");
        Sender<String> toOutside = c.sender(C);
        c.subscribe(A, s -> {
            toOutside.send("before " + s);
            if (s.equals("bad")) {
                throw new IllegalStateException("broken");
            }
            seen.add(s);
        }, 16);

        bus.publish(A, "bad");
        bus.publish(A, "good");
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> tick.run(later()));
        assertEquals("broken", thrown.getMessage());

        outside.drain();
        assertEquals(List.of("before bad"), out, "what was sent before the throw was still handed over");
        assertTrue(tick.hasPending(), "the message after the one that threw is still queued, not dropped");
        tick.run(later());
        assertEquals(List.of("good"), seen);
    }

    // ------------------------------------------------------------------------------------------- outside the tick

    @Test
    void subscribersOutsideTheTickReceiveItsSendsWhenItEndsAndInOrder() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Pump gui = bus.pump();
        List<String> drawn = new ArrayList<>();
        gui.subscribe(C, drawn::add, 16);
        Inbox editor = tick.inbox("editor");
        Inbox view = tick.inbox("view");
        Sender<String> toView = editor.sender(B);
        Sender<String> draw = view.sender(C);
        List<Integer> queuedDuringTheTick = new ArrayList<>();
        editor.subscribe(A, toView::send, 16);
        view.subscribe(B, s -> {
            draw.send("one " + s);
            queuedDuringTheTick.add(gui.hasPending() ? 1 : 0);
            draw.send("two " + s);
        }, 16);

        bus.publish(A, "x");
        tick.run(later());
        gui.drain();

        assertEquals(List.of(0), queuedDuringTheTick, "nothing outside the tick sees a send before the tick ends");
        assertEquals(List.of("one x", "two x"), drawn, "and then it sees all of them, in the order they were sent");
    }

    /**
     * The torn frame, made to happen if it can: a logic tick writes a set of values every run, and another tick on
     * another thread (a frame's) runs as fast as it can. If the hand-over between them were a message at a time, some
     * frame would see part of a set. It must see every set whole.
     */
    @Test
    void anotherTickReceivesEachTickOfSendsWholeNeverPartOfOne() throws Exception {
        Atchung bus = Atchung.create();
        Topic<Integer> writes = Topic.of("test.writes", Integer.class);
        int perTick = 32;
        int ticks = 2_000;

        Tick logic = bus.tick(null);
        Inbox model = logic.inbox("model");
        Sender<Integer> write = model.sender(writes);
        model.subscribe(A, s -> {
            int tickNo = Integer.parseInt(s);
            for (int i = 0; i < perTick; i++) {
                write.send(tickNo);
            }
        }, 16);

        Tick frame = bus.tick(null);
        int[] counts = new int[ticks];
        frame.inbox("view").subscribe(writes, v -> counts[v]++, perTick * ticks);

        java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
        List<String> torn = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread frames = new Thread(() -> {
            while (!done.get() || frame.hasPending()) {
                java.util.Arrays.fill(counts, 0);
                frame.run(LATER);
                for (int t = 0; t < ticks; t++) {
                    if (counts[t] != 0 && counts[t] != perTick) {
                        torn.add("tick " + t + ": " + counts[t] + " of " + perTick);
                    }
                }
            }
        }, "frames");
        frames.start();
        for (int t = 0; t < ticks; t++) {
            bus.publish(A, Integer.toString(t));
            logic.run(LATER);
        }
        done.set(true);
        frames.join(30_000);

        assertEquals(List.of(), torn, "a frame never runs on part of a tick's writes");
    }

    @Test
    void aTopicPrintsItselfAsTheRecordItWas() {
        assertEquals("Topic[name=test.a, payloadType=class java.lang.String]", A.toString(),
                "atchung 1.3's Topic was a record, and anything that read its toString reads the same text");
    }

    @Test
    void aSendOutsideAnyDeliveryIsAMessageFromOutside() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        List<String> seen = new ArrayList<>();
        Inbox editor = tick.inbox("editor");
        tick.inbox("view").subscribe(B, seen::add, 16);
        Sender<String> begin = editor.sender(B);

        begin.send("begin");

        assertEquals(List.of(), seen);
        tick.run(later());
        assertEquals(List.of("begin"), seen, "a component's constructor sends before any tick, and it waits for one");
    }

    @Test
    void anInboxMadeAfterTheFirstRunIsRefused() {
        Tick tick = Atchung.create().tick(null);
        tick.inbox("first");
        tick.run(later());

        assertThrows(IllegalStateException.class, () -> tick.inbox("late"));
    }

    // ------------------------------------------------------------------------------------------- cost

    @Test
    void aTickAllocatesNothingOnceWarm() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Inbox editor = tick.inbox("editor");
        Inbox view = tick.inbox("view");
        Sender<String> toView = editor.sender(B);
        int[] count = new int[1];
        editor.subscribe(A, toView::send, 64);
        view.subscribe(B, s -> count[0]++, 64);
        String message = "x";

        for (int i = 0; i < 20_000; i++) {
            bus.publish(A, message);
            tick.run(LATER);
        }
        com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < 10_000; i++) {
            bus.publish(A, message);
            tick.run(LATER);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        assertEquals(30_000, count[0]);
        assertTrue(allocated < 64 * 1024, "a publish and a two-round tick, ten thousand times, allocated "
                + allocated + " bytes: the delivery path must not allocate per message");
    }
}
