package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.internal.ActivityDispatcher;
import com.cajunsystems.boudin.internal.ActivityRegistry;
import com.cajunsystems.boudin.internal.BoudinEventLoop;
import com.cajunsystems.boudin.internal.HashedWheelTimer;
import com.cajunsystems.boudin.internal.WorkflowDispatcher;
import com.cajunsystems.boudin.internal.WorkflowRegistry;
import com.cajunsystems.gumbo.api.SharedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The main entry point for running workflow and activity implementations.
 *
 * <p>A {@code Worker} polls one task queue and executes the workflow and activity
 * types registered with it. Multiple workers can share the same task queue for
 * horizontal scaling; the shared log ensures each task is processed exactly once.
 *
 * <h2>Typical usage</h2>
 * <pre>{@code
 * var worker = Worker.newBuilder()
 *     .taskQueue("orders")
 *     .sharedLog(sharedLog)
 *     .build();
 *
 * worker.registerWorkflow(OrderWorkflowImpl.class);
 * worker.registerActivities(new OrderActivitiesImpl());
 * worker.start();
 *
 * // Later, for graceful shutdown:
 * worker.close();
 * }</pre>
 *
 * <p>The worker starts two subsystems:
 * <ul>
 *   <li>{@link WorkflowDispatcher} — subscribes to the workflow task queue and
 *       drives workflow execution (including crash recovery on startup).</li>
 *   <li>{@link ActivityDispatcher} — subscribes to the activity task queue and
 *       executes activity implementations on virtual threads.</li>
 * </ul>
 */
public class Worker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final String taskQueue;
    private final SharedLog sharedLog;
    private final WorkflowRegistry workflowRegistry;
    private final ActivityRegistry activityRegistry;
    private final BoudinEventLoop eventLoop;
    private final HashedWheelTimer timerWheel;
    private final WorkflowDispatcher workflowDispatcher;
    private final ActivityDispatcher activityDispatcher;

    private volatile boolean started = false;

    private Worker(Builder builder) {
        this.taskQueue = Objects.requireNonNull(builder.taskQueue, "taskQueue is required");
        this.sharedLog = Objects.requireNonNull(builder.sharedLog, "sharedLog is required");
        this.workflowRegistry = new WorkflowRegistry();
        this.activityRegistry = new ActivityRegistry();
        this.eventLoop = new BoudinEventLoop(builder.taskQueue);
        this.timerWheel = new HashedWheelTimer(eventLoop);
        this.workflowDispatcher = new WorkflowDispatcher(taskQueue, sharedLog, workflowRegistry, eventLoop);
        this.activityDispatcher = new ActivityDispatcher(taskQueue, sharedLog, activityRegistry);
    }

    /**
     * Registers a workflow implementation class.
     * Must be called before {@link #start()}.
     *
     * @param workflowImplClass a class that implements a {@code @WorkflowInterface}-annotated
     *                          interface and has a public no-arg constructor
     */
    public Worker registerWorkflow(Class<?> workflowImplClass) {
        workflowRegistry.register(workflowImplClass);
        return this;
    }

    /**
     * Registers one or more activity implementation instances.
     * Must be called before {@link #start()}.
     *
     * @param activityImpls instances of classes that implement {@code @ActivityInterface}-annotated interfaces
     */
    public Worker registerActivities(Object... activityImpls) {
        for (Object impl : activityImpls) {
            activityRegistry.register(impl);
        }
        return this;
    }

    /**
     * Starts the worker. Performs crash recovery (replays in-flight workflows),
     * then begins polling for new workflow and activity tasks.
     *
     * <p>This method returns after subscriptions are established — it does not block.
     * The worker runs background virtual threads.
     */
    public void start() {
        if (started) throw new IllegalStateException("Worker already started");
        started = true;
        log.info("Starting Worker on task queue '{}'", taskQueue);

        // Start activity dispatcher first so it's ready before workflows start scheduling
        activityDispatcher.start();

        // Start workflow dispatcher (performs crash recovery then goes live)
        workflowDispatcher.start();

        log.info("Worker started on task queue '{}'", taskQueue);
    }

    /**
     * Stops the worker and releases resources.
     * In-progress workflow virtual threads are interrupted.
     */
    @Override
    public void close() {
        log.info("Stopping Worker on task queue '{}'", taskQueue);
        workflowDispatcher.stop();
        activityDispatcher.stop();
        eventLoop.close(); // drain after subscriptions are closed
    }

    public String taskQueue() { return taskQueue; }

    public BoudinEventLoop eventLoop() { return eventLoop; }

    public HashedWheelTimer timerWheel() { return timerWheel; }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskQueue;
        private SharedLog sharedLog;

        private Builder() {}

        public Builder taskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
            return this;
        }

        public Builder sharedLog(SharedLog sharedLog) {
            this.sharedLog = sharedLog;
            return this;
        }

        public Worker build() {
            return new Worker(this);
        }
    }
}
