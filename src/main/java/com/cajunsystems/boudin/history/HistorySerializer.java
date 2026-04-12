package com.cajunsystems.boudin.history;

import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.serialization.LogSerializer;

/**
 * Kryo-backed {@link LogSerializer} for {@link HistoryEvent} objects.
 *
 * <p>Used as the serializer for {@code TypedLogView<HistoryEvent>} instances that
 * read and write the workflow history and task queue tags in the shared log.
 *
 * <p>Delegates to {@link KryoSerializer} which uses {@code writeClassAndObject}/
 * {@code readClassAndObject} so the full sealed type hierarchy is preserved
 * through serialization round-trips without explicit type registration.
 */
public class HistorySerializer implements LogSerializer<HistoryEvent> {

    /** Shared instance — safe to use concurrently (KryoSerializer is thread-safe). */
    public static final HistorySerializer INSTANCE = new HistorySerializer();

    @Override
    public byte[] serialize(HistoryEvent event) {
        return KryoSerializer.toBytes(event);
    }

    @Override
    public HistoryEvent deserialize(byte[] data) {
        return KryoSerializer.fromBytes(data);
    }
}
