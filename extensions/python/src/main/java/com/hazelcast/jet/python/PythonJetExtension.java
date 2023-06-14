package com.hazelcast.jet.python;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.ext.yaml.JobBuilder;
import com.hazelcast.jet.ext.yaml.JobBuilderException;
import com.hazelcast.jet.ext.yaml.JobBuilderExtension;
import com.hazelcast.logging.ILogger;

public class PythonJetExtension implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {

        jobBuilder.registerTransform("python", PythonJetExtension::transformPython);
        jobBuilder.registerTransform("debug", PythonJetExtension::debug);
    }

    private static Object transformPython(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        // not implemented for now
        return stageContext;
    }

    private static Object debug(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        // OK what are we allowed to do exactly on Viridian?
        String path = System.getenv("PATH");
        logger.info("PATH=" + path);

        return stageContext;
    }
}
