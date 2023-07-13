package com.hazelcast.jet.yaml;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.logging.ILogger;

public class SinkStep {

    private final String name;
    private final Consumer4<Object, String, YamlMapping, ILogger> function;

    public SinkStep(String name, Consumer4<Object, String, YamlMapping, ILogger> function) {
        this.name = name;
        this.function = function;
    }

    public String getName() {
        return name;
    }

    public Consumer4<Object, String, YamlMapping, ILogger> getFunction() {
        return function;
    }
}
