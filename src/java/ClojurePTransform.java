package datasplash.fns;

import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class ClojurePTransform extends PTransform<PInput,POutput> {

    private static final long serialVersionUID = 0;
    private final IFn bodyFn;
           
    public ClojurePTransform(IFn body_fn) {
        super();
        bodyFn = body_fn;
        
        
    }
    public POutput expand (PInput input) {
	POutput res = (POutput)  bodyFn.invoke(input);
	return res ;
    }
    
    
}
