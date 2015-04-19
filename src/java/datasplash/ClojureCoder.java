package datasplash.coders;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import datasplash.vals.ClojureVal;

import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CoderException;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public final class ClojureCoder extends CustomCoder<ClojureVal> {

    private static final long serialVersionUID = 0;
    private final IFn require;

    private final IFn thaw;
    private final IFn freeze;

    @JsonCreator
    public static ClojureCoder of() {
        return INSTANCE;
    }

    private static final ClojureCoder INSTANCE = new ClojureCoder();
    private ClojureCoder() {
        require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("taoensso.nippy"));
        thaw = Clojure.var("taoensso.nippy", "thaw-from-in!");
        freeze = Clojure.var("taoensso.nippy", "freeze-to-out!");
    }

    @Override
    public void encode(ClojureVal value, OutputStream outStream, Context context)
        throws IOException, CoderException {
        DataOutputStream dataOut = new DataOutputStream(outStream);
        freeze.invoke(dataOut, value.getValue());
    }

    @Override
    public ClojureVal decode(InputStream inStream, Context context)
        throws IOException, CoderException {
        DataInputStream dataIn = new DataInputStream(inStream);
        Object value = thaw.invoke(inStream);
        return new ClojureVal(value);
    }
}
