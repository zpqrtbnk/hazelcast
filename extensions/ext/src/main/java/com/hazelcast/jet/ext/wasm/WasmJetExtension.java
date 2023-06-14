package com.hazelcast.jet.ext.wasm;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.ext.yaml.JobBuilder;
import com.hazelcast.jet.ext.yaml.JobBuilderException;
import com.hazelcast.jet.ext.yaml.JobBuilderExtension;
import com.hazelcast.jet.ext.yaml.YamlUtils;
import com.hazelcast.jet.pipeline.StreamSourceStage;
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
