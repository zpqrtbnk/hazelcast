package com.hazelcast.jet.debug;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.yaml.JobBuilder;
import com.hazelcast.jet.yaml.JobBuilderException;
import com.hazelcast.jet.yaml.JobBuilderExtension;
import com.hazelcast.logging.ILogger;

public class JetDebugExtension implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {
        jobBuilder.registerTransform("debug", JetDebugExtension::debug);
    }

    private static Object debug(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        // OK what are we allowed to do exactly on Viridian?
        String path = System.getenv("PATH");
        logger.info("PATH=" + path);

        return stageContext;
    }
}
