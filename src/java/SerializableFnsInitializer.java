package datasplash.harness;

import org.apache.beam.sdk.harness.JvmInitializer;

/**
 * Allows Clojure fns to be deserialized on Beam workers.
 *
 * <p>Since Clojure 1.12.7, fns only (de)serialize when {@code clojure.serializable.fns=true},
 * and Clojure reads the property once, when its runtime loads. Worker harnesses run {@code
 * onStartup} before deserializing any user code. This class must therefore not reference any
 * Clojure class.
 */
public final class SerializableFnsInitializer implements JvmInitializer {

    private static final String PROPERTY = "clojure.serializable.fns";

    @Override
    public void onStartup() {
        if (System.getProperty(PROPERTY) == null) {
            System.setProperty(PROPERTY, "true");
        }
    }
}
