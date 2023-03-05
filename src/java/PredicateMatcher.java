package datasplash.testing;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class PredicateMatcher<T> extends BaseMatcher<T> {
    private final IFn pred;

    protected PredicateMatcher(IFn pred) {
        this.pred = pred;
    }

    @Override
    public boolean matches(Object o) {
        return RT.booleanCast(pred.invoke(o));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("satisfies predicate fonction: "
                + pred.getClass().getName());
    }

    @Override
    public void describeMismatch(Object item, Description description) {
        description.appendText("f(")
            .appendValue(item)
            .appendText(") was falsy");
    }

    public static Matcher<Object> satisfies(IFn pred) {
        return new PredicateMatcher<Object>(pred);
    }
}
