package datasplash.fns;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import clojure.lang.IFn;
import java.io.Serializable;

public final class ClojureSerializableFn implements Serializable, SerializableFunction<Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn Fn;

    public ClojureSerializableFn(IFn fn) {
        super();
        Fn = fn;
    }

    public Object apply(Object input) {
        return Fn.invoke(input);
    }
}
