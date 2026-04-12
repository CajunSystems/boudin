package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry that maps workflow type names to their implementation factories and methods.
 *
 * <p>A workflow type name is derived from the {@link WorkflowInterface}-annotated interface's
 * simple name (e.g., {@code "OrderWorkflow"}). This name is stored in
 * {@link com.cajunsystems.boudin.history.HistoryEvent.WorkflowStarted} so the correct
 * implementation class is instantiated when the workflow executes.
 *
 * <p>Thread-safe: may be called concurrently from multiple workflow dispatcher threads.
 */
public class WorkflowRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRegistry.class);

    private record WorkflowEntry(
            Supplier<Object> factory,
            Class<?> workflowInterface,
            Method workflowMethod
    ) {}

    private final ConcurrentHashMap<String, WorkflowEntry> workflows = new ConcurrentHashMap<>();

    /**
     * Registers a workflow implementation class. The class must implement exactly one
     * {@link WorkflowInterface}-annotated interface, which must have exactly one
     * {@link WorkflowMethod}-annotated method.
     */
    public void register(Class<?> implClass) {
        Class<?> workflowInterface = findWorkflowInterface(implClass);
        Method workflowMethod = findAnnotatedMethod(workflowInterface, WorkflowMethod.class);
        String workflowType = workflowInterface.getSimpleName();

        Supplier<Object> factory = () -> {
            try {
                return implClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Cannot instantiate workflow impl " + implClass.getName() +
                        ". It must have a public no-arg constructor.", e);
            }
        };

        workflows.put(workflowType, new WorkflowEntry(factory, workflowInterface, workflowMethod));
        log.debug("Registered workflow: {} -> {}", workflowType, implClass.getName());
    }

    /**
     * Creates a fresh instance of the workflow implementation for the given type name.
     */
    public Object createInstance(String workflowType) {
        WorkflowEntry entry = requireEntry(workflowType);
        return entry.factory().get();
    }

    /**
     * Returns the {@link WorkflowMethod}-annotated method for the given workflow type.
     */
    public Method findWorkflowMethod(String workflowType) {
        return requireEntry(workflowType).workflowMethod();
    }

    /**
     * Returns the {@link WorkflowInterface} for the given workflow type name.
     */
    public Class<?> findWorkflowInterface(String workflowType) {
        return requireEntry(workflowType).workflowInterface();
    }

    /**
     * Finds and returns a {@link SignalMethod}-annotated method with the given signal name
     * from the workflow interface. Returns null if not found.
     */
    public Method findSignalMethod(String workflowType, String signalName) {
        WorkflowEntry entry = requireEntry(workflowType);
        for (Method method : entry.workflowInterface().getDeclaredMethods()) {
            SignalMethod ann = method.getAnnotation(SignalMethod.class);
            if (ann == null) continue;
            String name = ann.name().isBlank() ? method.getName() : ann.name();
            if (name.equals(signalName)) return method;
        }
        return null;
    }

    /**
     * Finds and returns a {@link QueryMethod}-annotated method with the given query name.
     * Returns null if not found.
     */
    public Method findQueryMethod(String workflowType, String queryName) {
        WorkflowEntry entry = requireEntry(workflowType);
        for (Method method : entry.workflowInterface().getDeclaredMethods()) {
            QueryMethod ann = method.getAnnotation(QueryMethod.class);
            if (ann == null) continue;
            String name = ann.name().isBlank() ? method.getName() : ann.name();
            if (name.equals(queryName)) return method;
        }
        return null;
    }

    public boolean isRegistered(String workflowType) {
        return workflows.containsKey(workflowType);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private WorkflowEntry requireEntry(String workflowType) {
        WorkflowEntry entry = workflows.get(workflowType);
        if (entry == null) {
            throw new IllegalArgumentException(
                    "No workflow registered for type: " + workflowType +
                    ". Registered: " + workflows.keySet());
        }
        return entry;
    }

    private static Class<?> findWorkflowInterface(Class<?> implClass) {
        for (Class<?> iface : implClass.getInterfaces()) {
            if (iface.isAnnotationPresent(WorkflowInterface.class)) return iface;
        }
        throw new IllegalArgumentException(
                implClass.getName() + " does not implement any @WorkflowInterface-annotated interface");
    }

    private static Method findAnnotatedMethod(Class<?> iface, Class<? extends java.lang.annotation.Annotation> annotationClass) {
        for (Method method : iface.getDeclaredMethods()) {
            if (method.isAnnotationPresent(annotationClass)) return method;
        }
        throw new IllegalArgumentException(
                iface.getName() + " has no @" + annotationClass.getSimpleName() + "-annotated method");
    }
}
