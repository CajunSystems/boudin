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
 * and return a value. Query methods must not modify workflow state.
 *
 * <p>Note: Query methods are not yet fully implemented in Boudin v0.1.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QueryMethod {
    /**
     * Optional override for the query name. Defaults to the method name.
     */
    String name() default "";
}
