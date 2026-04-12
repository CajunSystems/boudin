package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an interface as an activity definition.
 *
 * <p>An activity interface defines a set of operations that can be invoked
 * from within a workflow. Activities are the units of work that interact
 * with external systems (databases, APIs, services). Unlike workflow code,
 * activities are not required to be deterministic.
 *
 * <p>Example:
 * <pre>{@code
 * @ActivityInterface
 * public interface OrderActivities {
 *     @ActivityMethod
 *     ValidationResult validateOrder(Order order);
 *
 *     @ActivityMethod
 *     PaymentResult chargePayment(Order order, PaymentDetails payment);
 *
 *     @ActivityMethod
 *     ShipmentResult shipOrder(Order order);
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ActivityInterface {
}
