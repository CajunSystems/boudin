package com.cajunsystems.boudin;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression guard for the lost-notification window that made {@code mvn verify} hang.
 *
 * <p>Under gumbo 0.2.0, {@code SharedLogService.subscribe} registered a subscription, then read
 * and delivered the existing backlog on a virtual thread, and only afterwards set
 * {@code backlogDone} — while the append path skipped any subscriber whose backlog was not yet
 * done. An entry appended after the backlog snapshot but before {@code backlogDone} was
 * delivered by neither path: the backlog loop never saw it, the notifier refused it, and it was
 * lost to that subscriber forever. A Boudin caller waiting on a terminal event then blocked
 * indefinitely.
 *
 * <p>gumbo 0.3.0 replaced the mechanism — one ordered delivery thread per subscription, and an
 * append path that only enqueues rather than deciding whether a subscriber is "ready" — and
 * 0.6.0 is what Boudin now depends on.
 *
 * <p>This test holds the window open deliberately: the backlog loop invokes the listener
 * synchronously, so a listener that takes time to run keeps a subscription mid-backlog for as
 * long as it runs. It <strong>fails</strong> (10s with zero deliveries) against gumbo 0.2.0 and
 * passes on 0.6.0, so it fails again if the dependency is ever rolled back or the delivery
 * guarantee regresses.
 */
class SubscriptionCatchUpTest {

    /** How long the listener spends on each backlog entry, holding the window open. */
    private static final long LISTENER_WORK_MS = 400;

    @Test
    void entryAppendedWhileBacklogIsDeliveringIsStillReceived() throws Exception {
        try (SharedLogService sharedLog = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build())) {

            LogTag tag = LogTag.of("race", "subscribe-window");
            LogView view = sharedLog.getView(tag);

            // One pre-existing entry, so the backlog loop has something to be busy with.
            view.append("backlog-0".getBytes()).join();

            CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
            SharedLog.Subscription sub = view.subscribe(LogPosition.BEGINNING, entry -> {
                received.add(new String(entry.data()));
                sleep(LISTENER_WORK_MS);   // backlogDone stays false while this runs
            });

            // Let the backlog thread take its snapshot and enter the (slow) delivery loop.
            await().atMost(5, TimeUnit.SECONDS).until(() -> received.contains("backlog-0"));

            // This append lands after the snapshot but before backlogDone — the window.
            view.append("the-important-one".getBytes()).join();

            try {
                await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> received.contains("the-important-one"));
                assertThat(received).contains("the-important-one");
            } finally {
                sub.close();
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
