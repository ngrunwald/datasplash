package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.util.Map;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class ClojureDoFn extends DoFn<Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn doFn;
    private final IFn windowFn;
    private final IFn startBundleFn;
    private final IFn finishBundleFn;

    public ClojureDoFn(Map<String, IFn>  fns_map) {
        super();
        doFn = fns_map.get("dofn");
        windowFn = fns_map.get("window-fn");
        startBundleFn = fns_map.get("start-bundle");
        finishBundleFn = fns_map.get("finish-bundle");
    }

    @ProcessElement
    public void processElement(ProcessContext c , BoundedWindow w){
        doFn.invoke(c);
        windowFn.invoke(w);
    }
}
