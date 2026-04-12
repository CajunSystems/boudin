package com.cajunsystems.boudin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the main entry point method of a workflow.
 *
 * <p>A workflow interface must have exactly one method annotated with {@code @WorkflowMethod}.
 * This method is invoked when a workflow is started via {@code WorkflowClient} and its
 * return value becomes the workflow's result.
 *
 * <p>The method may accept any number of serializable parameters and return any
 * serializable type (or void).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface WorkflowMethod {
    /**
     * Optional override for the workflow type name used in the event history.
     * Defaults to the workflow interface's simple name.
     */
    String name() default "";
}
