package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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
        validateQueryMethods(workflowInterface);
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

        if (workflows.containsKey(workflowType)) {
            throw new IllegalArgumentException(
                    "Workflow type '" + workflowType + "' is already registered. " +
                    "Existing: " + workflows.get(workflowType).workflowInterface().getName());
        }
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
        return findByWireName(requireEntry(workflowType).workflowInterface(),
                WireNames::signalName, signalName);
    }

    /**
     * Finds and returns a {@link QueryMethod}-annotated method with the given query name.
     * Returns null if not found.
     */
    public Method findQueryMethod(String workflowType, String queryName) {
        return findByWireName(requireEntry(workflowType).workflowInterface(),
                WireNames::queryName, queryName);
    }

    /**
     * Resolves a wire name against the interface's public methods.
     *
     * <p>Uses {@code getMethods()} rather than {@code getDeclaredMethods()} so a signal or query
     * inherited from a super-interface resolves. The client proxy reads the annotation off the
     * invoked method and does not care where it was declared, so a worker that only scanned
     * declared methods rejected calls the interface genuinely exposes.
     */
    private static Method findByWireName(Class<?> iface,
                                         Function<Method, String> nameOf,
                                         String wanted) {
        for (Method method : iface.getMethods()) {
            String name = nameOf.apply(method);
            if (name != null && name.equals(wanted)) return method;
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

    /**
     * Validates every {@link QueryMethod} on the interface: each must return a value (a query
     * that returns nothing cannot report state), and no two may resolve to the same query name.
     *
     * <p>Scans {@code getMethods()}, so queries inherited from a super-interface are validated
     * too — they are equally invocable, since the client reads the annotation off whichever
     * method the proxy was handed.
     */
    private static void validateQueryMethods(Class<?> iface) {
        Map<String, Method> byName = new HashMap<>();
        for (Method method : iface.getMethods()) {
            String name = WireNames.queryName(method);
            if (name == null) continue;

            if (method.getReturnType() == Void.TYPE) {
                throw new IllegalArgumentException(
                        method.getDeclaringClass().getName() + "#" + method.getName() +
                        " is annotated @QueryMethod but returns void; " +
                        "query methods must return the state being queried.");
            }

            Method existing = byName.put(name, method);
            // Same name and parameters means one overrides the other, not a name clash —
            // getMethods() can surface both the declaration and its override.
            if (existing != null && !isSameSignature(existing, method)) {
                throw new IllegalArgumentException(
                        iface.getName() + " has two @QueryMethod methods resolving to the query " +
                        "name '" + name + "': " + existing.getName() + " and " + method.getName() +
                        ". Query names must be unique; use @QueryMethod(name=...) to disambiguate.");
            }
        }
    }

    private static boolean isSameSignature(Method a, Method b) {
        return a.getName().equals(b.getName())
                && Arrays.equals(a.getParameterTypes(), b.getParameterTypes());
    }

    private static Method findAnnotatedMethod(Class<?> iface,
            Class<? extends java.lang.annotation.Annotation> annotationClass) {
        List<Method> matches = Arrays.stream(iface.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(annotationClass))
                .toList();
        if (matches.isEmpty()) {
            throw new IllegalArgumentException(
                    iface.getName() + " has no @" + annotationClass.getSimpleName() + "-annotated method");
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException(
                    iface.getName() + " has " + matches.size() + " @" +
                    annotationClass.getSimpleName() + "-annotated methods; exactly one is required. " +
                    "Found: " + matches.stream().map(Method::getName).toList());
        }
        return matches.getFirst();
    }
}
