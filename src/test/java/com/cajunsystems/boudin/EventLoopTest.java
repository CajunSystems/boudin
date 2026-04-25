package com.cajunsystems.boudin;

import com.cajunsystems.boudin.api.Worker;
import com.cajunsystems.boudin.internal.BoudinEventLoop;
import com.cajunsystems.boudin.internal.HashedWheelTimer;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopTest {

    @Test
    void tasksExecuteInOrder() throws Exception {
        BoudinEventLoop loop = new BoudinEventLoop("test");
        List<Integer> results = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        loop.submit(() -> { results.add(1); latch.countDown(); });
        loop.submit(() -> { results.add(2); latch.countDown(); });
        loop.submit(() -> { results.add(3); latch.countDown(); });

        assertTrue(latch.await(5, SECONDS));
        assertThat(results).containsExactly(1, 2, 3);
        loop.close();
    }

    @Test
    void timerFiresAfterDelay() throws Exception {
        BoudinEventLoop loop = new BoudinEventLoop("test");
        HashedWheelTimer timer = new HashedWheelTimer(loop);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong firedAt = new AtomicLong();

        long scheduledAt = System.currentTimeMillis();
        timer.schedule("t1", 100, () -> {
            firedAt.set(System.currentTimeMillis());
            latch.countDown();
        });

        assertTrue(latch.await(5, SECONDS));
        assertThat(firedAt.get() - scheduledAt).isGreaterThanOrEqualTo(90L);
        assertThat(timer.isPending("t1")).isFalse();
        loop.close();
    }

    @Test
    void cancelledTimerDoesNotFire() throws Exception {
        BoudinEventLoop loop = new BoudinEventLoop("test");
        HashedWheelTimer timer = new HashedWheelTimer(loop);
        AtomicBoolean fired = new AtomicBoolean(false);

        timer.schedule("t-cancel", 200, () -> fired.set(true));
        assertThat(timer.isPending("t-cancel")).isTrue();
        timer.cancel("t-cancel");
        assertThat(timer.isPending("t-cancel")).isFalse();

        Thread.sleep(300);
        assertThat(fired.get()).isFalse();
        loop.close();
    }

    @Test
    void workerExposesEventLoopAndTimerWheel() throws Exception {
        SharedLogService sharedLog = SharedLogService.open(
                SharedLogConfig.builder().persistenceAdapter(new InMemoryPersistenceAdapter()).build());
        Worker worker = Worker.newBuilder().taskQueue("test").sharedLog(sharedLog).build();

        assertThat(worker.eventLoop()).isNotNull();
        assertThat(worker.timerWheel()).isNotNull();

        worker.start();
        worker.close();
        sharedLog.close();
    }
}
