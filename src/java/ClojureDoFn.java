package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;

import clojure.lang.IFn;

public final class ClojureDoFn extends DoFn<Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn doFn;

    public ClojureDoFn(IFn fn) {
        super();
        doFn = fn;
    }

    @ProcessElement
    public void processElement(ProcessContext c ){
        doFn.invoke(c);
    }
}
