package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.util.Map;
import java.util.HashMap;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public class ClojureDoFn extends AbstractClojureDoFn {

    private static final long serialVersionUID = 1L;

    private transient Object system = null;

    public ClojureDoFn(Map<String, IFn> fns_map) {
        super(fns_map);
    }

    @Setup
    public void initialize() {
        if (initializeFn != null) {
            system = initializeFn.invoke();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow w) {
        HashMap<String, Object> extra = new HashMap<String, Object>();
        extra.put("window", w);
        extra.put("system", system);
        doFn.invoke(c, extra);
        windowFn.invoke(w);
    }
}
