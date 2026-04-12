package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.annotation.ActivityInterface;
import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps activity type keys to their implementation instances and methods.
 *
 * <p>An activity type key has the form {@code "InterfaceName#methodName"}, for example
 * {@code "OrderActivities#chargePayment"}. This key is stored in
 * {@link com.cajunsystems.boudin.history.HistoryEvent.ActivityScheduled} so the correct
 * implementation is found during execution.
 *
 * <p>Thread-safe: may be called concurrently from multiple activity dispatcher threads.
 */
public class ActivityRegistry {

    private static final Logger log = LoggerFactory.getLogger(ActivityRegistry.class);

    private record ActivityEntry(Object impl, Method method) {}

    private final ConcurrentHashMap<String, ActivityEntry> activities = new ConcurrentHashMap<>();

    /**
     * Registers all {@link ActivityMethod}-annotated methods found on the given
     * implementation object's {@link ActivityInterface}-annotated interfaces.
     */
    public void register(Object activityImpl) {
        Class<?> implClass = activityImpl.getClass();
        for (Class<?> iface : implClass.getInterfaces()) {
            if (!iface.isAnnotationPresent(ActivityInterface.class)) continue;
            for (Method method : iface.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(ActivityMethod.class)) continue;
                String key = activityTypeKey(iface, method);
                activities.put(key, new ActivityEntry(activityImpl, method));
                log.debug("Registered activity: {}", key);
            }
        }
    }

    /**
     * Invokes the activity identified by {@code activityType} with the given
     * Kryo-serialized argument bytes. Returns the Kryo-serialized result bytes,
     * or null bytes if the method returns void.
     *
     * @throws IllegalArgumentException if the activity type is not registered
     * @throws Exception                if the activity method throws
     */
    public byte[] invoke(String activityType, byte[] inputBytes) throws Exception {
        ActivityEntry entry = activities.get(activityType);
        if (entry == null) {
            throw new IllegalArgumentException(
                    "No activity registered for type: " + activityType +
                    ". Registered: " + activities.keySet());
        }

        // Deserialize args and invoke
        Object[] args = (Object[]) KryoSerializer.fromBytes(inputBytes);
        if (args == null) args = new Object[0];

        Object result = entry.method().invoke(entry.impl(), args);

        // Serialize result (null for void methods)
        if (entry.method().getReturnType() == Void.TYPE) {
            return null;
        }
        return KryoSerializer.toBytes(result);
    }

    /**
     * Returns true if an activity with the given type key is registered.
     */
    public boolean isRegistered(String activityType) {
        return activities.containsKey(activityType);
    }

    /**
     * Builds the canonical activity type key for a given interface + method.
     * Format: {@code "InterfaceName#methodName"}
     */
    public static String activityTypeKey(Class<?> activityInterface, Method method) {
        return activityInterface.getSimpleName() + "#" + method.getName();
    }
}
