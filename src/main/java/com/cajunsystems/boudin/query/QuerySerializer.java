package com.cajunsystems.boudin.query;

import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.serialization.LogSerializer;

/**
 * Kryo-backed {@link LogSerializer} for {@link QueryMessage} objects on the
 * {@code workflow-queries:{workflowId}} tag.
 *
 * <p>Mirrors {@link com.cajunsystems.boudin.history.HistorySerializer} — delegates to
 * {@link KryoSerializer}, which uses {@code writeClassAndObject}/{@code readClassAndObject}
 * so the sealed type is preserved through a round trip without explicit registration.
 */
public class QuerySerializer implements LogSerializer<QueryMessage> {

    /** Shared instance — safe to use concurrently (KryoSerializer is thread-safe). */
    public static final QuerySerializer INSTANCE = new QuerySerializer();

    @Override
    public byte[] serialize(QueryMessage message) {
        return KryoSerializer.toBytes(message);
    }

    @Override
    public QueryMessage deserialize(byte[] data) {
        return KryoSerializer.fromBytes(data);
    }
}
