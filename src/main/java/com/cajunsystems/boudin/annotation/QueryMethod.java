package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a query handler in a workflow.
 *
 * <p>Query methods allow external code to read the current state of a running
 * workflow without modifying it. Unlike signal methods, queries are synchronous
 * and return a value.
 *
 * <p>Requirements, enforced when the workflow is registered on a {@code Worker}:
 * <ul>
 *   <li>A query method must return a value — {@code void} is rejected.</li>
 *   <li>Query names (the method name, or {@link #name()} when set) must be unique
 *       within a workflow interface.</li>
 *   <li>Handlers <strong>must not</strong> modify workflow state. This cannot be enforced;
 *       a mutating handler will corrupt deterministic replay.</li>
 *   <li>Handlers <strong>must not</strong> block. They run while holding the workflow's signal
 *       lock, so a blocking handler stalls signal delivery to that workflow.</li>
 * </ul>
 *
 * <p>Queries travel on the {@code workflow-queries:{workflowId}} log tag, never the workflow
 * history, so they leave replay and determinism untouched. Handlers run while holding the
 * workflow's signal lock, so a query never interleaves with a signal handler — but it may
 * observe the workflow thread between yield points.
 *
 * <p>A query is answered only while a worker is running the workflow. Querying a completed
 * workflow, or one whose task queue has no worker, fails with
 * {@code WorkflowQueryException(errorType = "QueryTimedOut")}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QueryMethod {
    /**
     * Optional override for the query name. Defaults to the method name.
     */
    String name() default "";
}
