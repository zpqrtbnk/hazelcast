/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.jobbuilder;

import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;

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
