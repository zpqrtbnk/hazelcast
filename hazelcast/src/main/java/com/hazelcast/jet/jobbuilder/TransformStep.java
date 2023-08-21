package com.hazelcast.jet.jobbuilder;

import com.hazelcast.logging.ILogger;

public class TransformStep {

    private final String name;
    private final Function4<Object, String, InfoMap, ILogger, Object> function;

    public TransformStep(String name, Function4<Object, String, InfoMap, ILogger, Object> function) {
        this.name = name;
        this.function = function;
    }

    public String getName() {
        return name;
    }

    public Function4<Object, String, InfoMap, ILogger, Object> getFunction() {
        return function;
    }
}
