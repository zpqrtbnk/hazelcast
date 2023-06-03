package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.yaml.JobBuilderExtension;
import com.hazelcast.jet.yaml.JobBuilder;
import com.hazelcast.jet.yaml.JobBuilderException;
import com.hazelcast.jet.yaml.YamlUtils;
import com.hazelcast.logging.ILogger;

public class DotnetJetExtension implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {
        jobBuilder.addTransform("dotnet", DotnetJetExtension::transformDotnet);
    }

    private static Object transformDotnet(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(YamlUtils.getProperty(properties, "dotnet-dir"))
                .withDotnetExe(YamlUtils.getProperty(properties, "dotnet-exe"))
                .withTransformName(YamlUtils.getProperty(properties,"dotnet-method"))
                .withPreserveOrder(YamlUtils.getProperty(properties, "preserve-order", true));

        YamlMapping parallelism = properties.childAsMapping("parallelism");
        if (parallelism != null) {
            config.withParallelism(
                    YamlUtils.getProperty(parallelism, "processors"),
                    YamlUtils.getProperty(parallelism, "operations"));
        }

        // FIXME this, stageContext, timestamps should already have been taken care of
        StreamStage streamStage = stageContext instanceof StreamStage ? (StreamStage) stageContext :
                stageContext instanceof StreamSourceStage ? ((StreamSourceStage) stageContext).withoutTimestamps() :
                null;

        if (streamStage == null)
            throw new JobBuilderException("panic: unsupported stage type");

        return streamStage
                .apply(DotnetTransforms.mapAsync((service, input) -> {
                    DeserializingEntry entry = (DeserializingEntry) input;
                    Data[] data = new Data[2];
                    data[0] = DeserializingEntryExtensions.getDataKey(entry);
                    data[1] = DeserializingEntryExtensions.getDataValue(entry);
                    return service
                            .mapAsync(data)
                            .thenApply(x -> DeserializingEntryExtensions.createNew(entry, x[0], x[1]));
                }, config))
                .setLocalParallelism(config.getLocalParallelism());
    }
}
