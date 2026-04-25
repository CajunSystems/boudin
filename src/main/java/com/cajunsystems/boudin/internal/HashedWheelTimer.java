package com.cajunsystems.boudin.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Named timer registry backed by {@link BoudinEventLoop}.
 *
 * Provides schedule-by-ID and cancel-by-ID semantics. All timer callbacks fire
 * on the event loop thread. Used by {@code Workflow.sleep()} in Phase 4 and by
 * activity timeout enforcement in Phase 5.
 */
public class HashedWheelTimer {

    private static final Logger log = LoggerFactory.getLogger(HashedWheelTimer.class);

    private final BoudinEventLoop eventLoop;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pendingTimers =
            new ConcurrentHashMap<>();

    public HashedWheelTimer(BoudinEventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    /**
     * Schedules a timer to fire after {@code delayMs} milliseconds.
     * If a timer with the same ID already exists, it is cancelled and replaced.
     *
     * @param timerId  unique identifier for this timer
     * @param delayMs  delay in milliseconds before the callback fires
     * @param onFire   callback invoked on the event loop thread when the timer fires
     */
    public void schedule(String timerId, long delayMs, Runnable onFire) {
        cancel(timerId);
        ScheduledFuture<?> future = eventLoop.schedule(() -> {
            pendingTimers.remove(timerId);
            log.debug("Timer {} fired", timerId);
            onFire.run();
        }, delayMs);
        pendingTimers.put(timerId, future);
        log.debug("Timer {} scheduled for {}ms", timerId, delayMs);
    }

    /**
     * Cancels a pending timer. No-op if the timer does not exist or has already fired.
     */
    public void cancel(String timerId) {
        ScheduledFuture<?> existing = pendingTimers.remove(timerId);
        if (existing != null) {
            existing.cancel(false);
            log.debug("Timer {} cancelled", timerId);
        }
    }

    /** Returns true if a timer with the given ID is currently scheduled but not yet fired. */
    public boolean isPending(String timerId) {
        return pendingTimers.containsKey(timerId);
    }
}
