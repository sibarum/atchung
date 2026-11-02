package sibarum.atchung;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An {@link Alarm} whose time moves only when a test says so: {@link #advance} publishes everything that came due, in
 * the order it came due, and the next {@link Tick#run} delivers it. So a test of a five-second bound takes no five
 * seconds, and gives the same result every time.
 *
 * <pre>{@code
 * HandClock clock = new HandClock(bus);
 * tick.alarm(clock).whenOverdue(Tick.Overdue.FAIL);   // a default bound running out fails the test
 * ...
 * clock.advance(Duration.ofMillis(300));
 * tick.run(later());
 * }</pre>
 */
public final class HandClock implements Alarm {

    private final Atchung bus;
    private final List<Due<?>> pending = new ArrayList<>();
    private long now;
    private long order;

    public HandClock(Atchung bus) {
        this.bus = Objects.requireNonNull(bus, "bus");
    }

    @Override
    public synchronized <T> Cancel after(Duration delay, Topic<T> topic, T message) {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(message, "message");
        if (delay.isZero() || delay.isNegative()) {
            throw new IllegalArgumentException("an alarm waits some time: " + delay + " on " + topic.name());
        }
        Due<T> due = new Due<>(now + delay.toNanos(), order++, topic, message);
        pending.add(due);
        return () -> {
            synchronized (HandClock.this) {
                pending.remove(due);
            }
        };
    }

    /** Move time on by {@code by}, publishing what comes due on the way, earliest first. */
    public void advance(Duration by) {
        Objects.requireNonNull(by, "by");
        if (by.isNegative()) {
            throw new IllegalArgumentException("time moves forward: " + by);
        }
        long until;
        synchronized (this) {
            until = now + by.toNanos();
        }
        while (true) {
            Due<?> next;
            synchronized (this) {
                next = earliest(until);
                if (next == null) {
                    now = until;
                    return;
                }
                pending.remove(next);
                now = next.at;
            }
            next.publish(bus);
        }
    }

    /** How many alarms are waiting. */
    public synchronized int pending() {
        return pending.size();
    }

    /** How far time has moved since this clock was made. */
    public synchronized Duration elapsed() {
        return Duration.ofNanos(now);
    }

    private Due<?> earliest(long until) {
        Due<?> best = null;
        for (Due<?> d : pending) {
            if (d.at <= until && (best == null || d.at < best.at || (d.at == best.at && d.order < best.order))) {
                best = d;
            }
        }
        return best;
    }

    private record Due<T>(long at, long order, Topic<T> topic, T message) {
        void publish(Atchung bus) {
            bus.publish(topic, message);
        }
    }
}
