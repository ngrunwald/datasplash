package datasplash.fns;

import datasplash.fns.ClojureDoFn;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import datasplash.coder.NippyCoder;
import java.util.HashMap;
import java.util.Map;
import clojure.lang.IFn;


public final class ClojureStatefulDoFn extends AbstractClojureDoFn {

    private static final long serialVersionUID = 1L;

    @StateId("state")
    private final StateSpec<ValueState<Object>> stateSpec = StateSpecs.value(new NippyCoder());
    private transient Object system = null;

    public ClojureStatefulDoFn(Map<String, IFn> fns_map) {
        super(fns_map);
    }

    @Setup
    public void initialize() {
        if (initializeFn != null) {
            system = initializeFn.invoke();
        }
     }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow w, @StateId("state") ValueState<Object> state) {
        HashMap<String, Object> extra = new HashMap<String, Object>();
        extra.put("state", state);
        extra.put("window", w);
        extra.put("system", system);
        doFn.invoke(c, extra);
        windowFn.invoke(w);
    }
}
