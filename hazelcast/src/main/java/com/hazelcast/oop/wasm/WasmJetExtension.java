package com.hazelcast.oop.wasm;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.yaml.JobBuilder;
import com.hazelcast.jet.yaml.JobBuilderException;
import com.hazelcast.jet.yaml.JobBuilderExtension;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.logging.ILogger;

public class WasmJetExtension implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {

        // TODO: enable if it ever works
        //jobBuilder.registerTransform("wasm", WasmJetExtension::transformWasm);
    }

    private static Object transformWasm(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        StreamStage streamStage = (StreamStage) stageContext;

        return streamStage
                .apply(WasmTransforms.mapAsync((service, input) -> {
                    return service.mapAsync((Data[]) null);
                }))
                .setLocalParallelism(1);
    }
}
