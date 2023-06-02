package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.yaml.JetExtension;
import com.hazelcast.jet.yaml.JobBuilder;
import com.hazelcast.jet.yaml.JobBuilderException;
import com.hazelcast.jet.yaml.YamlUtils;
import com.hazelcast.logging.ILogger;

public class DotnetJetExtension implements JetExtension {

    @Override
    public void register(JobBuilder jobBuilder) {
        jobBuilder.addTransform("dotnet", DotnetJetExtension::transformDotnet);
    }

    private static Object transformDotnet(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(YamlUtils.getProperty(properties, "dotnet-dir"))
                .withDotnetExe(YamlUtils.getProperty(properties, "dotnet-exe"))
                .withMethodName(YamlUtils.getProperty(properties,"dotnet-method"))
                .withPreserveOrder(YamlUtils.getProperty(properties, "preserve-order", true));

        YamlMapping parallelism = properties.childAsMapping("parallelism");
        if (parallelism != null) {
            config.withParallelism(
                    YamlUtils.getProperty(parallelism, "processors"),
                    YamlUtils.getProperty(parallelism, "operations"));
        }

        if (stageContext instanceof StreamStage) {
            // FIXME how can we check types?
            return ((StreamStage) stageContext)
                    .apply(DotnetTransforms.mapAsync((service, input) -> {
                        DeserializingEntry entry = (DeserializingEntry) input;
                        Data[] rawInput = new Data[2];
                        rawInput[0] = DeserializingEntryExtensions.getDataKey(entry);
                        rawInput[1] = DeserializingEntryExtensions.getDataValue(entry);
                        return service.mapAsync(rawInput);
                    }, config))
                    .setLocalParallelism(config.getLocalParallelism());
        }
        else if (stageContext instanceof StreamSourceStage) {
            // FIXME timestamp is a requirement of sources
            //   so that should not be a case, the source should always produce a stream stage
            // FIXME how can we check?
            return ((StreamSourceStage) stageContext).withoutTimestamps()
                    .apply(DotnetTransforms.mapAsync((service, input) -> {
                        DeserializingEntry entry = (DeserializingEntry) input;
                        Data[] rawInput = new Data[2];
                        rawInput[0] = DeserializingEntryExtensions.getDataKey(entry);
                        rawInput[1] = DeserializingEntryExtensions.getDataValue(entry);
                        return service.mapAsync(rawInput);
                    }, config))
                    .setLocalParallelism(config.getLocalParallelism());
        }
        else if (stageContext instanceof GeneralStage) {
            throw new JobBuilderException("panic: general stage?");
        }
        else {
            throw new JobBuilderException("panic");
        }
    }
}
