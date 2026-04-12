package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an interface as a workflow definition.
 *
 * <p>A workflow interface defines the contract for a durable, long-running process.
 * The interface should contain exactly one {@link WorkflowMethod}-annotated method,
 * and optionally {@link SignalMethod} and {@link QueryMethod} methods.
 *
 * <p>Example:
 * <pre>{@code
 * @WorkflowInterface
 * public interface OrderWorkflow {
 *     @WorkflowMethod
 *     OrderResult processOrder(Order order);
 *
 *     @SignalMethod
 *     void cancelOrder(String reason);
 *
 *     @QueryMethod
 *     OrderStatus getStatus();
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WorkflowInterface {
}
