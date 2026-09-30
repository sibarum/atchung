package sibarum.atchung;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateTest {

    // An immutable state value.
    record Counter(int n) {
        Counter plus(int by) {
            return new Counter(n + by);
        }
    }

    @Test
    void startsAtVersionZeroWithInitial() {
        State<Counter> s = State.of(new Counter(0)).build();
        assertEquals(0L, s.version());
        assertEquals(new Counter(0), s.value());
    }

    @Test
    void commitAppliesDeclaredMutationAndBumpsVersion() {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        s.commit(add, 5);
        s.commit(add, 3);

        assertEquals(new Counter(8), s.value());
        assertEquals(2L, s.version());
    }

    @Test
    void undeclaredCommitterIsRejected() {
        State<Counter> s = State.of(new Counter(0)).build();
        Committer<Counter, Integer> foreign =
                State.of(new Counter(0)).mutation("add", (c, by) -> c.plus(by));
        assertThrows(IllegalArgumentException.class, () -> s.commit(foreign, 1));
    }

    @Test
    void onCommitFiresPerVersionAndIsPausableLossy() {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        List<Long> versions = new ArrayList<>();
        Subscription sub = s.onCommit(snap -> versions.add(snap.version()));

        s.commit(add, 1);            // v1
        sub.pause();
        s.commit(add, 1);            // v2 — not observed
        sub.resume();
        s.commit(add, 1);            // v3

        assertEquals(List.of(1L, 3L), versions, "commits during pause are not delivered");
        // ...but state stayed coherent: on resume, current() is truth regardless of the missed notify.
        assertEquals(3L, s.version());
        assertEquals(new Counter(3), s.value());
    }

    @Test
    void historyIsBoundedByDepth() {
        State.Builder<Counter> b = State.of(new Counter(0)).history(3);
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        for (int i = 0; i < 10; i++) {
            s.commit(add, 1);
        }
        assertEquals(10L, s.version());
        assertTrue(s.at(10L).isPresent(), "latest retained");
        assertTrue(s.at(8L).isPresent());
        assertFalse(s.at(5L).isPresent(), "old versions evicted by depth bound");
    }

    @Test
    void noHistoryByDefault() {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();
        s.commit(add, 1);
        assertFalse(s.at(1L).isPresent(), "no history kept unless requested");
    }

    @Test
    void awaitReturnsImmediatelyWhenAlreadyAhead() throws Exception {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();
        s.commit(add, 1);
        assertEquals(1L, s.await(0L).version());
    }

    @Test
    void awaitParksUntilNextCommit() throws Exception {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            s.commit(add, 7);
        });
        producer.start();

        Versioned<Counter> got = s.await(0L); // blocks until v1
        assertEquals(1L, got.version());
        assertEquals(new Counter(7), got.value());
        producer.join();
    }

    @Test
    void concurrentProducersKeepVersionsGaplessAndMonotonic() throws Exception {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        int threads = 6;
        int per = 500;
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                for (int i = 0; i < per; i++) {
                    s.commit(add, 1);
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }

        assertEquals((long) threads * per, s.version(), "one version per commit, no gaps");
        assertEquals(new Counter(threads * per), s.value(), "no lost updates under CAS commit");
    }

    @Test
    void commitIfChangedSkipsAnEqualValueAndWakesNobody() {
        State.Builder<Counter> b = State.of(new Counter(3));
        Committer<Counter, Integer> set = b.mutation("set", (c, n) -> new Counter(n));
        State<Counter> s = b.build();
        List<Long> seen = new ArrayList<>();
        s.onCommit(snap -> seen.add(snap.version()));

        assertFalse(s.commitIfChanged(set, 3));
        assertEquals(0L, s.version());
        assertTrue(seen.isEmpty());

        assertTrue(s.commitIfChanged(set, 4));
        assertEquals(1L, s.version());
        assertEquals(List.of(1L), seen);
    }

    @Test
    void onCommitLatestNeverOverlapsNorGoesBackwardsAndEndsOnTheNewest() throws Exception {
        State.Builder<Counter> b = State.of(new Counter(0));
        Committer<Counter, Integer> add = b.mutation("add", (c, by) -> c.plus(by));
        State<Counter> s = b.build();

        java.util.concurrent.atomic.AtomicInteger inside = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger overlapped = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger backwards = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicLong last = new java.util.concurrent.atomic.AtomicLong();
        s.onCommitLatest(snap -> {
            if (inside.incrementAndGet() > 1) {
                overlapped.incrementAndGet();
            }
            if (snap.version() <= last.get()) {
                backwards.incrementAndGet();
            }
            last.set(snap.version());
            inside.decrementAndGet();
        });

        int threads = 8;
        int per = 300;
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                for (int i = 0; i < per; i++) {
                    s.commit(add, 1);
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }

        assertEquals(0, overlapped.get(), "never two deliveries at once");
        assertEquals(0, backwards.get(), "strictly increasing versions");
        assertEquals((long) threads * per, last.get(), "ends on the newest");
    }
}
