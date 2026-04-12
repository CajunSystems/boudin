package com.cajunsystems.boudin.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

/**
 * Thread-safe Kryo serializer for arbitrary Java objects.
 *
 * <p>Uses a {@link Pool} of {@link Kryo} instances to allow concurrent serialization
 * without the overhead of creating a new Kryo instance per call. Works well with
 * virtual threads since the pool is bounded and Kryo instances are lightweight.
 *
 * <p>Serializes with full class information ({@code writeClassAndObject}) so
 * deserialization does not need to know the expected type in advance. This supports
 * polymorphic types, complex object graphs, generics, and arbitrary domain objects
 * with no annotations required.
 */
public final class KryoSerializer {

    private static final Pool<Kryo> POOL = new Pool<>(true, false, 16) {
        @Override
        protected Kryo create() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(true);
            return kryo;
        }
    };

    private KryoSerializer() {}

    /**
     * Serializes any object to bytes. Includes full class information so the object
     * can be deserialized without knowing its type upfront.
     *
     * @param obj the object to serialize (may be null)
     * @return serialized bytes
     */
    public static byte[] toBytes(Object obj) {
        Kryo kryo = POOL.obtain();
        try (Output output = new Output(256, -1)) {
            kryo.writeClassAndObject(output, obj);
            return output.toBytes();
        } finally {
            POOL.free(kryo);
        }
    }

    /**
     * Deserializes an object from bytes. The type is determined from the embedded
     * class information written by {@link #toBytes}.
     *
     * @param data the bytes to deserialize (may be null or empty → returns null)
     * @param <T>  the expected type (unchecked cast)
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromBytes(byte[] data) {
        if (data == null || data.length == 0) return null;
        Kryo kryo = POOL.obtain();
        try (Input input = new Input(data)) {
            return (T) kryo.readClassAndObject(input);
        } finally {
            POOL.free(kryo);
        }
    }
}
