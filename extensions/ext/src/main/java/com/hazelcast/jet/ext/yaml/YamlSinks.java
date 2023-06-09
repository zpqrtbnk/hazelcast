package com.hazelcast.jet.ext.yaml;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.logging.ILogger;

public class YamlSinks implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {
        jobBuilder.registerSink("map", YamlSinks::sinkMap);
    }

    private static void sinkMap(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        String mapName = YamlUtils.getProperty(properties, "map-name");
        logger.fine("  map-name: " + mapName);
        if (stageContext instanceof GeneralStage) {
            // FIXME how can we check?
            ((GeneralStage)stageContext).writeTo(Sinks.map(mapName));
        }
        else {
            throw new JobBuilderException("panic");
        }
    }
}
