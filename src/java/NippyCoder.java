package datasplash.coder;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import java.io.DataOutputStream;
import java.io.DataInputStream;

public final class NippyCoder extends org.apache.beam.sdk.coders.CustomCoder {

    private final IFn freeze;
    private final IFn thaw;

    public NippyCoder() {
        super();
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("taoensso.nippy"));
        freeze = Clojure.var("taoensso.nippy", "freeze-to-out!");
        thaw = Clojure.var("taoensso.nippy", "thaw-from-in!");
    }
     public void encode(Object obj, java.io.OutputStream os) {
        DataOutputStream dos = new DataOutputStream(os);
        try{
        freeze.invoke(dos, obj);
        }
        catch(IllegalStateException e){
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("taoensso.nippy"));
        this.encode(obj, os);}
    }

    public Object decode(java.io.InputStream is) {
        DataInputStream dis = new DataInputStream(is);
        try{
            return thaw.invoke(dis);
        }
        catch(IllegalStateException e){
       IFn require = Clojure.var("clojure.core", "require");
       require.invoke(Clojure.read("taoensso.nippy"));
       return this.decode(is);}
    }

    public void verifyDeterministic() {
    }
}
