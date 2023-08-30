package com.hazelcast.jet.jobbuilder;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.usercode.compile.InMemoryFileManager;
import com.hazelcast.usercode.compile.JavaSourceFromString;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.util.Collections;
import java.util.UUID;

public class BuiltinStageProvider implements JobBuilderStageProvider {

    @Override
    public SourceStage[] getSources() {
        return new SourceStage[] {
            new SourceStage("map-journal", BuiltinStageProvider::sourceMapJournal)
        };
    }

    @Override
    public TransformStage[] getTransforms() {
        return new TransformStage[] {
            new TransformStage("identity", BuiltinStageProvider::identity)
        };
    }

    @Override
    public SinkStage[] getSinks() {
        return new SinkStage[] {
            new SinkStage("map", BuiltinStageProvider::sinkMap)
        };
    }

    private static Object sourceMapJournal(Pipeline pipelineContext, String name, JobBuilderInfoMap definition, ILogger logger) throws JobBuilderException {

        String mapName = definition.childAsString("map-name");
        String initialPositionString = definition.childAsString("journal-initial-position");
        JournalInitialPosition initialPosition = JournalInitialPosition.valueOf(initialPositionString);
        StreamSource<?> streamSource = Sources.mapJournal(mapName, initialPosition);

        StreamSourceStage<?> sourceStage = pipelineContext.readFrom(streamSource);
        StreamStage<?> stage;

        String timestamps = definition.childAsString("timestamps", "NONE");
        switch (timestamps.toUpperCase()) {
            case "NONE":
                stage = sourceStage.withoutTimestamps();
                break;
            case "NATIVE":
                long nativeAllowedLag = definition.childAsLong("timestamps-allowed-lag");
                stage = sourceStage.withNativeTimestamps(nativeAllowedLag);
                break;
            case "INGESTION":
                stage = sourceStage.withIngestionTimestamps();
                break;
            default:
                // TODO: consider supporting lambda expressions?
                // we don't support lambda expressions for now
                //long lambdaAllowedLag = definition.childAsLong("timestamps-allowed-lag");
                throw new JobBuilderException("Invalid timestamps value '" + timestamps + "', must be 'NONE', 'NATIVE' or 'INGESTION'.");
        }

        return stage;
    }

    private static Object identity(Object stageContext, String name, JobBuilderInfoMap properties, ILogger logger) {
        return stageContext;
    }

    private static void sinkMap(Object stageContext, String name, JobBuilderInfoMap definition, ILogger logger) throws JobBuilderException {

        String mapName = definition.childAsString("map-name");
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
