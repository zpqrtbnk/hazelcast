package com.hazelcast.jet.jobbuilder;

import com.hazelcast.logging.ILogger;

public class SinkStep {

    private final String name;
    private final Consumer4<Object, String, InfoMap, ILogger> function;

    public SinkStep(String name, Consumer4<Object, String, InfoMap, ILogger> function) {
        this.name = name;
        this.function = function;
    }

    public String getName() {
        return name;
    }

    public Consumer4<Object, String, InfoMap, ILogger> getFunction() {
        return function;
    }
}
