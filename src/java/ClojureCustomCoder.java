package datasplash.fns;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class ClojureCustomCoder extends CustomCoder {

    private static final long serialVersionUID = 0;
    private final IFn encodeFn;
    private final IFn decodeFn;
       
    public ClojureCustomCoder(Map<String, IFn>  fns_map ) {
        super();
        encodeFn = fns_map.get("encode-fn");
        decodeFn = fns_map.get("decode-fn");
        
    }
    public void encode (Object obj, OutputStream out) {
	encodeFn.invoke(obj,out) ;
    }
    
    public Object decode ( InputStream in) {
	return decodeFn.invoke(in) ;
    }
    

    public void verifyDeterministic () {
    }
    public boolean consistentWithEquals () {
	return true ;
    }
}
