package com.hazelcast.foo;

import java.util.function.Function;

//public interface Lambda0Runner {
//    Object run();
//}
//
//public interface Lambda1Runner {
//    Object run(Object arg0);
//}
//
//public class Lambda1RunnerImpl implements Lambda1Runner {
//    private final Function<Object, Object> f;
//    public Lambda1Runner(Function<Object, Object> f) { this.f = f; }
//    public Object run(Object arg0) { return f(arg0); }
//}
//
//public class Lambdas {
//    public final Lambda1Runner l = new Lambda1RunnerImpl(x -> x);
//    public Map<String, Object> runners;
//}

// each java code fragment from the job definition
//
// - transform: map
//   args:
//     - Object
//   returns: Object
//   code: |
//     x -> x.toString()
//
// ends up being a function of a JobCode class
//
// public class JobCode {
//   public final Function<Object, Object> f4 = x -> x.toString();
// }
//
// now how to invoke it?
//
// either each Lambda goes into its own CompiledLambda4 (implements/extends Lambda1)
// so we can cas to Lambda1 and then l.apply(arg)
// but what about types? we cannot really handle the types?
// or shall we write the ENTIRE pipeline as Java source AND compile it at once?
//
// QUESTION:
//   should the jobBuilder build the pipeline as it goes (what we have now)
//   OR transpile to Java code which can then be compiled, and that would allow... tons of stuff?
//   but requires that the JDK be installed on Viridian and what's installed really?

public class Foo1 {

    // but then... foo() cannot be referenced from a static context
    //private static final Function<Object, Object> f1 = $$LAMBDA$$;

    // that is OK
    private final Function<Object, Object> f1 = $$LAMBDA$$;

    public String meh() {
        return "hello";
    }

    public <T,R> R gmeh(T input) {
        // error: String cannot be converted to 'R'
        //return input.toString();

        // works
        return (R) input.toString();
    }

    public Object invokeLambda(Object input) {
        Function<Object, Object> f = $$LAMBDA$$;
        return f.apply(input);
    }

    private Object foo(Object x) {
        return x;
    }
}