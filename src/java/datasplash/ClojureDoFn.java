package datasplash.fns;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import datasplash.vals.ClojureVal;
import clojure.lang.IFn;

public final class ClojureDoFn extends DoFn<Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn doFn;

    public ClojureDoFn(IFn fn) {
        super();
        doFn = fn;
    }

    @Override
    public void processElement(ProcessContext c){
        doFn.invoke(c);
    }
}
