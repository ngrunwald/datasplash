package datasplash.fns;

import org.apache.beam.sdk.coders.CustomCoder;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class ClojureCustomCoder extends CustomCoder {

    private static final long serialVersionUID = 0;
    private final IFn encodeFn;
    private final IFn decodeFn;

    public ClojureCustomCoder(Map<String, IFn> fns_map) {
        super();
        encodeFn = fns_map.get("encode-fn");
        decodeFn = fns_map.get("decode-fn");
    }

    public void encode(Object obj, OutputStream out) {
        encodeFn.invoke(obj, new DataOutputStream(out));
    }

    public Object decode(InputStream in) {
        return decodeFn.invoke(new DataInputStream(in));
    }

    public boolean consistentWithEquals() {
        return true;
    }

    public void verifyDeterministic() {
        return;
    }
}
