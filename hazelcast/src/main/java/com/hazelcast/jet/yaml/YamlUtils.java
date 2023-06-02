package com.hazelcast.jet.yaml;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlScalar;

public final class YamlUtils {

    public static <T> T getProperty(YamlMapping properties, String name) throws JobBuilderException {

        YamlScalar scalar = properties.childAsScalar(name);
        if (scalar == null) {
            throw new JobBuilderException("panic: missing " + name + " property");
        }

        // value can be: String Integer Long Double Boolean
        // how can we convert it to the expected value (eg if it's an enum?)

        return scalar.nodeValue();
    }

    public static <T> T getProperty(YamlMapping properties, String name, T defaultValue) {

        YamlScalar scalar = properties.childAsScalar(name);
        if (scalar == null) {
            return defaultValue;
        }

        // value can be: String Integer Long Double Boolean
        // how can we convert it to the expected value (eg if it's an enum?)

        return scalar.nodeValue();
    }
}
