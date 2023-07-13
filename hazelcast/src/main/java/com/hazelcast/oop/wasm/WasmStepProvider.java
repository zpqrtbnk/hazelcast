package com.hazelcast.oop.wasm;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.yaml.*;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.logging.ILogger;

public class WasmStepProvider implements StepProvider {

    @Override
    public SourceStep[] getSources() {
        return null;
    }

    @Override
    public TransformStep[] getTransforms() {
        //("wasm", transformWasm);
        return null;
    }

    @Override
    public SinkStep[] getSinks() {
        return new SinkStep[0];
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
