package com.hazelcast.jet.debug;

import com.hazelcast.jet.jobbuilder.*;
import com.hazelcast.logging.ILogger;

public class DebugStepProvider implements StepProvider {

    @Override
    public SourceStep[] getSources() {
        return null;
    }

    @Override
    public TransformStep[] getTransforms() {
        return new TransformStep[] {
            new TransformStep("debug", DebugStepProvider::debug)
        };
    }

    @Override
    public SinkStep[] getSinks() {
        return null;
    }

    private static Object debug(Object stageContext, String name, InfoMap properties, ILogger logger) throws JobBuilderException {

        // OK what are we allowed to do exactly on Viridian?
        String path = System.getenv("PATH");
        logger.info("PATH=" + path);

        return stageContext;
    }
}
