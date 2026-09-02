package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowMethod;

import java.lang.reflect.Method;

/**
 * Single source of truth for the name a method is known by on the wire.
 *
 * <p>Every annotation that names something in the log ({@code @WorkflowMethod},
 * {@code @SignalMethod}, {@code @QueryMethod}, {@code @ActivityMethod}) follows the same rule:
 * the annotation's {@code name} if set, otherwise the method name.
 *
 * <p>This exists because client and worker resolve the name <em>independently</em> — the client
 * reads the annotation off the invoked interface method, the worker scans the registered
 * interface for a match. Two copies of the rule that drift apart make every affected signal or
 * query silently unroutable, so there is exactly one copy.
 */
public final class WireNames {

    private WireNames() {}

    /** Returns the wire name for a {@code @SignalMethod}, or null if not annotated. */
    public static String signalName(Method method) {
        SignalMethod ann = method.getAnnotation(SignalMethod.class);
        return ann == null ? null : orMethodName(ann.name(), method);
    }

    /** Returns the wire name for a {@code @QueryMethod}, or null if not annotated. */
    public static String queryName(Method method) {
        QueryMethod ann = method.getAnnotation(QueryMethod.class);
        return ann == null ? null : orMethodName(ann.name(), method);
    }

    /** Returns the wire name for a {@code @WorkflowMethod}, or null if not annotated. */
    public static String workflowMethodName(Method method) {
        WorkflowMethod ann = method.getAnnotation(WorkflowMethod.class);
        return ann == null ? null : orMethodName(ann.name(), method);
    }

    /** Returns the wire name for an {@code @ActivityMethod}, or null if not annotated. */
    public static String activityName(Method method) {
        ActivityMethod ann = method.getAnnotation(ActivityMethod.class);
        return ann == null ? null : orMethodName(ann.name(), method);
    }

    private static String orMethodName(String annotationName, Method method) {
        return (annotationName == null || annotationName.isBlank())
                ? method.getName()
                : annotationName;
    }
}
