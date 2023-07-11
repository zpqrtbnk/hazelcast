package com.hazelcast.jet.python;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.yaml.JobBuilder;
import com.hazelcast.jet.yaml.JobBuilderException;
import com.hazelcast.jet.yaml.JobBuilderExtension;
import com.hazelcast.logging.ILogger;

public class PythonJetExtension implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {

        jobBuilder.registerTransform("python", PythonJetExtension::transformPython);
    }

    // - transform: python
    //   python-exe: name of the python exe (python, python3...)
    //   preserve-order: true|false
    //   etc...
    private static Object transformPython(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        // not implemented for now
        return stageContext;
    }
}
