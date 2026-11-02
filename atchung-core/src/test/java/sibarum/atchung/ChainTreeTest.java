package sibarum.atchung;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Chain} as a tree: waits (ALL, ANY, NONE) over branches (a message, a time, a chain), every wait bounded, and
 * failure as an outcome. Each test is one of vexelray-framework's {@code docs/plans/chains.md} rulings of 2026-11-02
 * made to fail if it stops being true; the ruling's number is in the test's name or its first line.
 */
class ChainTreeTest {

    record Typed(String text) {
    }

    record Request(int n) {
    }

    record Reply(int n) {
    }

    record Answer(String said) {
    }

    record Close() {
    }

    record Both(Reply left, Answer right) {
    }

    /** What every test's ledger knows: the last thing typed, what it did in order, and a run it holds. */
    record S(String last, String log, Chain.Resume waiting) {
        static final S EMPTY = new S("", "", null);

        S did(String what) {
            return new S(last, log + what + ";", waiting);
        }
    }

    private static final Topic<Typed> TYPED = Topic.of("test.typed", Typed.class);
    private static final Topic<Request> REQUESTS = Topic.of("test.requests", Request.class);
    private static final Topic<Reply> REPLIES = Topic.of("test.replies", Reply.class);
    private static final Topic<Answer> ANSWERS = Topic.of("test.answers", Answer.class);
    private static final Topic<Close> CLOSES = Topic.of("test.closes", Close.class);

    private static final Duration QUIET = Duration.ofMillis(300);

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

    /** A bus, a hand clock, one ledger, and the steps the tests compose. */
    private static final class Rig {
        final Atchung bus = Atchung.create();
        final HandClock clock = new HandClock(bus);
        final Tick tick = bus.tick(null).alarm(clock).whenOverdue(Tick.Overdue.FAIL);
        final Ledger.Builder<S> b = Ledger.of(S.EMPTY);

        /** Any outcome goes on, as the run's first step. */
        final Step<S, Object> start = b.step("start", (s, args, out) -> {
            out.result(args);
            return s.did("start " + name(args));
        });
        /** Whatever it is given, written down: the end of most runs here. */
        final Step<S, Object> took = b.step("took", (s, o, out) -> s.did("took " + name(o)));
        /** The same, giving it on as its outcome, for a chain whose end is a branch's arrival. */
        final Step<S, Object> pass = b.step("pass", (s, o, out) -> {
            out.result(o);
            return s.did("pass " + name(o));
        });
        final Step<S, Typed> typed = b.step("typed", (s, t, out) -> {
            out.result(t);
            return new S(t.text(), s.log, s.waiting).did("typed " + t.text());
        });
        final Step<S, Quiet> search = b.step("search", (s, q, out) -> s.did("search " + s.last));
        final Step<S, Request> ask = b.step("ask", (s, r, out) -> {
            out.send(REQUESTS, r);
            out.result(r);
            return s.did("ask " + r.n());
        });
        final Step<S, Object> hold = b.step("hold", (s, o, out) -> new S(s.last, s.log, out.hold()).did("hold"));
        final Step<S, Object> holdForPerson = b.step("holdForPerson",
                (s, o, out) -> new S(s.last, s.log, out.holdForPerson()).did("holdForPerson"));
        final Step<S, Answer> answered = b.step("answered", (s, a, out) -> {
            if (s.waiting != null) {
                out.resume(s.waiting, a);
            }
            return s.did("answered " + a.said());
        });
        Ledger<S> ledger;

        Rig() {
            b.on(ANSWERS, answered);
        }

        Ledger<S> ledger() {
            if (ledger == null) {
                ledger = b.build(tick.inbox("ledger"), "test.ledger", 64);
            }
            return ledger;
        }

        /** Requests answered at once, on the publisher's thread, as a job on a direct executor would. */
        void answerRequests() {
            bus.subscribeAsync(REQUESTS, r -> bus.publish(REPLIES, new Reply(r.n())), Runnable::run);
        }

        void settle() {
            tick.run(System.nanoTime() + 60_000_000_000L);
        }

        void later(Duration by) {
            clock.advance(by);
            settle();
        }

        void send(Object message) {
            if (message instanceof Typed t) {
                bus.publish(TYPED, t);
            } else if (message instanceof Answer a) {
                bus.publish(ANSWERS, a);
            } else if (message instanceof Reply r) {
                bus.publish(REPLIES, r);
            } else {
                bus.publish(CLOSES, (Close) message);
            }
            settle();
        }

        <A> void begin(Chain<S, A, ?> chain, A args) {
            bus.publish(chain.topic(), chain.with(args));
            settle();
        }

        String log() {
            return ledger.state().value().log();
        }
    }

    /** A framework outcome by its kind, anything else as it prints. */
    private static String name(Object o) {
        if (o instanceof TimedOut || o instanceof Missing || o instanceof Broken || o instanceof Quiet) {
            return o.getClass().getSimpleName();
        }
        return String.valueOf(o);
    }

    // ------------------------------------------------------------------------------------------- NONE

    @Test
    void searchAsYouTypeSearchesOnceTheTypingStops() {
        Rig r = new Rig();
        Chain.start("search", r.ledger(), r.typed).on(TYPED)
                .none(TYPED).within(QUIET)
                .then(Quiet.class, r.ledger, r.search)
                .build();

        r.send(new Typed("c"));
        r.later(Duration.ofMillis(100));
        r.send(new Typed("ca"));
        r.later(Duration.ofMillis(100));
        r.send(new Typed("cat"));
        r.later(QUIET);

        assertEquals("typed c;typed ca;typed cat;search cat;", r.log(),
                "each keystroke broke the run before it, and only the last one's window closed");
    }

    @Test
    void ruling10AWaitHearsOnlyWhatArrivesAfterItIsEntered() {
        Rig r = new Rig();
        Chain.start("search", r.ledger(), r.typed).on(TYPED)
                .none(TYPED).within(QUIET)
                .then(Quiet.class, r.ledger, r.search)
                .build();

        r.send(new Typed("x"));
        r.later(QUIET);

        assertEquals("typed x;search x;", r.log(), "the keystroke that started the run did not break its own window");
    }

    @Test
    void aBrokenNoneMayBeTakenByTheNextLink() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("unless", r.ledger(), r.start)
                .none(CLOSES).within(QUIET)
                .then(Quiet.class, r.ledger, r.took)
                .or(Broken.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.send(new Close());

        assertEquals("start go;took Broken;", r.log());
    }

    @Test
    void aNoneBeforeAMessageGoesOnWithThatMessage() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("until", r.ledger(), r.start)
                .none(TYPED).before(CLOSES)
                .then(Close.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.send(new Close());

        assertEquals("start go;took Close[];", r.log());
    }

    @Test
    void aNoneWithNoBoundIsRefusedWhenBuilt() {
        Rig r = new Rig();
        Chain.Builder<S, Object> b = Chain.start("unbounded", r.ledger(), r.start).none(TYPED);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, b::build);
        assertEquals("chain 'unbounded': none(test.typed) has no bound, and that nothing happened is only seen when "
                + "a window closes: say within(time) or before(topic)", thrown.getMessage());
    }

    // ------------------------------------------------------------------------------------------- ANY

    @Test
    void anAnyGoesOnWithTheFirstArrivalAndOnce() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("first", r.ledger(), r.start)
                .any(ANSWERS, CLOSES)
                .then(Object.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.send(new Close());
        r.send(new Close());

        assertEquals("start go;took Close[];", r.log(), "the second Close found no wait");
    }

    @Test
    void anAnyWithinATimeGoesOnWithTimedOutWhenNothingCame() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("timeout", r.ledger(), r.start)
                .any(REPLIES).within(Duration.ofSeconds(2))
                .then(Reply.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofSeconds(2));
        r.send(new Reply(1));

        assertEquals("start go;took TimedOut;", r.log(), "and the reply after it went nowhere");
    }

    @Test
    void ruling9AReplyFindsTheRunWhoseRequestItAnswers() {
        Rig r = new Rig();
        r.answerRequests();
        Chain<S, Request, Object> chain = Chain.start("open", r.ledger(), r.ask)
                .any(REPLIES)
                .then(Reply.class, r.ledger, r.took)
                .build();

        r.bus.publish(chain.topic(), chain.with(new Request(1)));
        r.bus.publish(chain.topic(), chain.with(new Request(2)));
        r.settle();   // the requests are handed to the job when the tick ends
        r.settle();   // and the replies come back in the next

        String log = r.log();
        assertTrue(log.contains("took Reply[n=1]") && log.contains("took Reply[n=2]"),
                "two opens at once each took their own reply, not the first one twice: " + log);
    }

    @Test
    void ruling9AMessageFromNoRunIsTakenByEveryRunWaitingForIt() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("broadcast", r.ledger(), r.start)
                .any(CLOSES)
                .then(Close.class, r.ledger, r.took)
                .build();

        r.bus.publish(chain.topic(), chain.with("a"));
        r.bus.publish(chain.topic(), chain.with("b"));
        r.settle();
        r.send(new Close());

        assertEquals("start a;start b;took Close[];took Close[];", r.log());
    }

    // ------------------------------------------------------------------------------------------- ALL

    @Test
    void anAllGoesOnWithEveryArrivalAsOneRecordWhateverTheOrder() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("both", r.ledger(), r.start)
                .all(REPLIES, ANSWERS).as(Both::new)
                .then(Both.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.send(new Answer("yes"));
        r.send(new Reply(7));

        assertEquals("start go;answered yes;took Both[left=Reply[n=7], right=Answer[said=yes]];", r.log());
    }

    @Test
    void anAllWhoseBoundRunsOutGoesOnWithWhatIsMissing() {
        Rig r = new Rig();
        Missing[] seen = new Missing[1];
        Ledger.Builder<S> b = r.b;
        Step<S, Missing> missing = b.step("missing", (s, m, out) -> {
            seen[0] = m;
            return s.did("missing");
        });
        Chain<S, Object, Object> chain = Chain.start("both", r.ledger(), r.start)
                .all(REPLIES, ANSWERS).as(Both::new).within(Duration.ofSeconds(1))
                .then(Both.class, r.ledger, r.took)
                .or(Missing.class, r.ledger, missing)
                .build();

        r.begin(chain, "go");
        r.send(new Reply(1));
        r.later(Duration.ofSeconds(1));

        assertEquals("start go;missing;", r.log());
        assertEquals(java.util.List.of("test.answers"), seen[0].missing());
    }

    // ------------------------------------------------------------------------------------------- bounds

    @Test
    void ruling3And7AHoldWhoseDefaultBoundRunsOutFailsUnderTheFailPolicy() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold)
                .then(Answer.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.clock.advance(Tick.DEFAULT_BOUND);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, r::settle);
        assertTrue(thrown.getMessage().startsWith("chain 'held' waited PT5S at hold, the tick's default bound, and it "
                + "ran out"), thrown.getMessage());
    }

    @Test
    void ruling3AHoldWhoseDefaultBoundRunsOutGoesOnWithTimedOutAndALateResumeIsRefused() {
        Rig r = new Rig();
        r.tick.whenOverdue(Tick.Overdue.TRACE);
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold)
                .then(Answer.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Tick.DEFAULT_BOUND);
        r.send(new Answer("late"));

        assertEquals("start go;hold;took TimedOut;answered late;", r.log(), "the late answer did not go on twice");
    }

    @Test
    void ruling5WithinOnTheLinkIsBehaviourNotABug() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold).within(Duration.ofSeconds(1))
                .then(Answer.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofSeconds(1));

        assertEquals("start go;hold;took TimedOut;", r.log(), "the fail policy heard nothing");
    }

    @Test
    void ruling5AChainsOwnDefaultIsStatedToo() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .boundedBy(Duration.ofSeconds(1))
                .then(Object.class, r.ledger, r.hold)
                .then(Answer.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofSeconds(1));

        assertEquals("start go;hold;took TimedOut;", r.log());
    }

    @Test
    void ruling5ForeverIsNeverBoundedAndStillResumes() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold).forever()
                .then(Answer.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofHours(1));
        r.send(new Answer("eventually"));

        assertEquals("start go;hold;answered eventually;took Answer[said=eventually];", r.log());
        assertEquals(0, r.clock.pending(), "forever armed no timer");
    }

    @Test
    void ruling4APersonsWaitIsBoundedByTheirAnswerNotATime() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("asked", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.holdForPerson)
                .then(Answer.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofHours(1));
        r.send(new Answer("ok"));

        assertEquals("start go;holdForPerson;answered ok;took Answer[said=ok];", r.log());
    }

    @Test
    void aRunResumedTwiceGoesOnOnce() {
        Rig r = new Rig();
        Chain<S, Object, Object> chain = Chain.start("held", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold)
                .then(Answer.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.send(new Answer("one"));
        r.send(new Answer("two"));

        assertEquals("start go;hold;answered one;took Answer[said=one];answered two;", r.log());
    }

    @Test
    void aWaitOnAMessageIsBoundedByTheTicksDefault() {
        Rig r = new Rig();
        r.tick.whenOverdue(Tick.Overdue.TRACE).defaultBound(Duration.ofSeconds(2));
        Chain<S, Object, Object> chain = Chain.start("default", r.ledger(), r.start)
                .any(REPLIES)
                .then(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(chain, "go");
        r.later(Duration.ofSeconds(2));

        assertEquals("start go;took TimedOut;", r.log());
    }

    @Test
    void aTickWithNoAlarmRefusesABoundByName() {
        Atchung bus = Atchung.create();
        Tick tick = bus.tick(null);
        Ledger.Builder<S> b = Ledger.of(S.EMPTY);
        Step<S, Object> start = b.step("start", (s, o, out) -> {
            out.result(o);
            return s;
        });
        Ledger<S> ledger = b.build(tick.inbox("ledger"), "test.ledger", 16);
        Chain<S, Object, Object> chain = Chain.start("noalarm", ledger, start).any(REPLIES).build();

        bus.publish(chain.topic(), chain.with("go"));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> tick.run(System.nanoTime() + 60_000_000_000L));
        assertEquals("inbox 'ledger' keeps a bound of PT5S, and its tick has no alarm to keep it with: give the tick "
                + "one (Tick.alarm), as the framework does", thrown.getMessage());
    }

    // ------------------------------------------------------------------------------------------- chains as branches

    @Test
    void ruling2AChainAsABranchArrivesWithWhatItEnds() {
        Rig r = new Rig();
        Chain<S, Object, Reply> fetch = Chain.start("fetch", r.ledger(), r.start)
                .any(REPLIES).forever()
                .then(Reply.class, r.ledger, r.pass)
                .build(Reply.class);
        Chain<S, Object, Object> outer = Chain.start("outer", r.ledger, r.start)
                .any(fetch.asBranch())
                .then(Reply.class, r.ledger, r.took)
                .build();

        r.begin(outer, "go");
        r.send(new Reply(3));

        assertEquals("start go;start go;pass Reply[n=3];took Reply[n=3];", r.log(),
                "the child started with the outcome the wait was entered with, and its end went on in the parent");
    }

    @Test
    void ruling8AChainBranchsDeadlineCutsItAndItArrivesWithTimedOut() {
        Rig r = new Rig();
        Chain<S, Object, Object> slow = Chain.start("slow", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold).forever()
                .then(Answer.class, r.ledger, r.pass)
                .build();
        Chain<S, Object, Object> outer = Chain.start("outer", r.ledger, r.start)
                .any(slow.asBranch().within(Duration.ofSeconds(10)))
                .then(Answer.class, r.ledger, r.took)
                .or(TimedOut.class, r.ledger, r.took)
                .build();

        r.begin(outer, "go");
        r.later(Duration.ofSeconds(10));
        r.send(new Answer("late"));

        assertEquals("start go;start go;hold;took TimedOut;answered late;", r.log(),
                "the late answer found the child cut, and nothing of it went on");
    }

    @Test
    void ruling8AnAnyCutsTheChainThatLost() {
        Rig r = new Rig();
        Chain<S, Object, Object> slow = Chain.start("slow", r.ledger(), r.start)
                .then(Object.class, r.ledger, r.hold).forever()
                .then(Answer.class, r.ledger, r.pass)
                .build();
        Chain<S, Object, Object> race = Chain.start("race", r.ledger, r.start)
                .any(Branch.of(CLOSES), slow.asBranch())
                .then(Object.class, r.ledger, r.took)
                .build();

        r.begin(race, "go");
        r.send(new Close());
        r.send(new Answer("too late"));

        assertEquals("start go;start go;hold;took Close[];answered too late;", r.log(),
                "the child that lost took no step after it was cut");
    }

    @Test
    void anAllOverTwoChainsIsTwoFlowsThatJoin() {
        Rig r = new Rig();
        Chain<S, Object, Reply> left = Chain.start("left", r.ledger(), r.start)
                .any(REPLIES).forever()
                .then(Reply.class, r.ledger, r.pass)
                .build(Reply.class);
        Chain<S, Object, Answer> right = Chain.start("right", r.ledger, r.start)
                .any(ANSWERS).forever()
                .then(Answer.class, r.ledger, r.pass)
                .build(Answer.class);
        Chain<S, Object, Object> both = Chain.start("both", r.ledger, r.start)
                .all(left.asBranch(), right.asBranch()).as(Both::new)
                .then(Both.class, r.ledger, r.took)
                .build();

        r.begin(both, "go");
        r.send(new Answer("a"));
        r.send(new Reply(9));

        assertTrue(r.log().endsWith("took Both[left=Reply[n=9], right=Answer[said=a]];"), r.log());
    }
}
