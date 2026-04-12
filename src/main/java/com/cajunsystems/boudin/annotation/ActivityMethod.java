package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as an activity that can be invoked from a workflow.
 *
 * <p>Activity methods may perform any operation including blocking I/O,
 * database calls, or HTTP requests. They are executed on virtual threads
 * so blocking is not a problem.
 *
 * <p>Activity results are durably stored in the workflow's event history,
 * so on replay (after a crash), the activity is not re-executed — the
 * cached result is returned instead.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ActivityMethod {
    /**
     * Optional override for the activity method name stored in the event history.
     * Defaults to the method name.
     */
    String name() default "";
}
