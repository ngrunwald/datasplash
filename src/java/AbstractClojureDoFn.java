package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.util.Map;
import java.util.HashMap;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public abstract class AbstractClojureDoFn extends DoFn<Object, Object> {

    protected static final long serialVersionUID = 0;
    protected final IFn doFn;
    protected final IFn windowFn;
    protected final IFn startBundleFn;
    protected final IFn finishBundleFn;
    protected final IFn initializeFn;

    public AbstractClojureDoFn(Map<String, IFn> fns_map) {
        super();
        doFn = fns_map.get("dofn");
        windowFn = fns_map.get("window-fn");
        startBundleFn = fns_map.get("start-bundle");
        finishBundleFn = fns_map.get("finish-bundle");
        initializeFn = fns_map.get("initialize-fn");
    }
}
