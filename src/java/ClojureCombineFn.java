package datasplash.fns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.coders.Coder;
import java.util.Map;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class ClojureCombineFn extends CombineFn<Object, Object, Object> {

    private static final long serialVersionUID = 0;
    private final IFn initFn;
    private final IFn extractFn;
    private final IFn reduceFn;
    private final IFn combineFn;
    private final IFn combineFnRaw;
    private final Coder accCoder;
    private final Coder outputCoder;
    
    public ClojureCombineFn(Map<String, IFn>  fns_map, Coder output_coder , Coder acc_coder ) {
        super();
        initFn = fns_map.get("init-fn");
        extractFn = fns_map.get("extract-fn");
        reduceFn = fns_map.get("reduce-fn");
        combineFn = fns_map.get("combine-fn");
	combineFnRaw = fns_map.get("combine-fn-raw");
	accCoder = acc_coder;
	outputCoder = output_coder;
    }
    public Object createAccumulator () {
	return initFn.invoke() ;
    }
    public Object addInput (Object acc , Object elt) {
	return reduceFn.invoke(acc, elt);
    }
    public Object mergeAccumulators (Iterable<Object> accs) {
	return combineFn.invoke(accs);
    }
    public Object extractOutput (Object acc) {
	return extractFn.invoke(acc);
    }
    

    public Coder getDefaultOutputCoder(Object a, Object b) {
	return outputCoder;
    }
    public Coder getAccumulatorCoder(Object a , Object b) {
	return accCoder;
    }

    public IFn getInitFn () {
	return initFn ;
    }
    public IFn getReduceFn () {
	return reduceFn ;
    }
    
    public IFn getMergeFn () {
	return combineFnRaw ;
    }
    
    public IFn getExtractFn () {
	return extractFn ;
    }
    
}
