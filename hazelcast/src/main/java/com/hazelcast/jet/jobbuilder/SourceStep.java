package com.hazelcast.jet.jobbuilder;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;

public class SourceStep {

    private final String name;
    private final Function4<Pipeline, String, InfoMap, ILogger, Object> function;

    public SourceStep(String name, Function4<Pipeline, String, InfoMap, ILogger, Object> function) {
        this.name = name;
        this.function = function;
    }

    public String getName() {
        return name;
    }

    public Function4<Pipeline, String, InfoMap, ILogger, Object> getFunction() {
        return function;
    }
}
