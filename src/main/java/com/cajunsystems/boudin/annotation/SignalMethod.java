package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a signal handler in a workflow.
 *
 * <p>Signal methods allow external code to send events to a running workflow.
 * They are called asynchronously and must return {@code void}. Signal methods
 * typically update the workflow's internal state (e.g., set flags or fields
 * that {@link Workflow#await} conditions check).
 *
 * <p>Signal methods may be called from outside the workflow via a workflow stub:
 * <pre>{@code
 * OrderWorkflow stub = client.newWorkflowStub(OrderWorkflow.class, options);
 * stub.startAsync(order);
 * // later...
 * stub.cancelOrder("customer request");
 * }</pre>
 *
 * <p>Signal methods are delivered in-order relative to other signals but may
 * interleave with activity completions.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SignalMethod {
    /**
     * Optional override for the signal name stored in the event history.
     * Defaults to the method name.
     */
    String name() default "";
}
