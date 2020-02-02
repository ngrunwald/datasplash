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

    @StateId("state")
    private final StateSpec<ValueState<Object>> stateSpec = StateSpecs.value(new NippyCoder());

    public ClojureStatefulDoFn(Map<String, IFn> fns_map) {
        super(fns_map);
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow w, @StateId("state") ValueState state) {
        HashMap extra = new HashMap();
        extra.put("state", state);
        extra.put("window", w);
        doFn.invoke(c, extra);
        windowFn.invoke(w);
    }
}
