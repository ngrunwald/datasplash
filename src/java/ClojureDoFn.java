package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.Window;

import clojure.lang.IFn;

public final class ClojureDoFn extends DoFn<Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn doFn;
    private final IFn windowFn;

    public ClojureDoFn(IFn fn) {
        super();
        doFn = fn;
        windowFn = null;
    }
    public ClojureDoFn(IFn fn, IFn wFn) {
        super();
        doFn = fn;
        windowFn = wFn;

    }

    @ProcessElement
    public void processElement(ProcessContext c ){
        doFn.invoke(c);
    }
    @ProcessElement
    public void processElement(ProcessContext c , Window w){
        doFn.invoke(c);
        windowFn.invoke(w);
    }
}
