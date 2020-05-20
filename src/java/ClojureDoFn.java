package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.util.Map;
import java.util.HashMap;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public class ClojureDoFn extends AbstractClojureDoFn {

    private transient Object system = null;

    public ClojureDoFn(Map<String, IFn> fns_map) {
        super(fns_map);
    }

    @Setup
    public void initialize() {
        if (initializeFn != null) {
            system = initializeFn.invoke();


        }}

    @ProcessElement
    public void processElement(ProcessContext c , BoundedWindow w){
        HashMap extra = new HashMap();
        extra.put("window", w);
        extra.put("system", system);
        doFn.invoke(c, extra);
        windowFn.invoke(w);
    }
}
