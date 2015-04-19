package datasplash.vals;

public final class ClojureVal {
    // private static final long serialVersionUID = 0;
    private final Object value;

    public ClojureVal(Object o) {
        super();
        value = o;
    }

    public final Object getValue () {
        return value;
    }
}
