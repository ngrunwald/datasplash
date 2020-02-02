package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.util.Map;
import java.util.HashMap;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public class ClojureDoFn extends AbstractClojureDoFn {

    public ClojureDoFn(Map<String, IFn> fns_map) {
        super(fns_map);
    }

    @ProcessElement
    public void processElement(ProcessContext c , BoundedWindow w){
        HashMap extra = new HashMap();
        extra.put("window", w);
        doFn.invoke(c, extra);
        windowFn.invoke(w);
    }
}
