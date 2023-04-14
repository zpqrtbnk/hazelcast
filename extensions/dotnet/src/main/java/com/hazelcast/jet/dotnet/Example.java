package com.hazelcast.jet.dotnet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

public class Example {

    public static void main(String[] args) {

        // these are not arguments, because they are closely related to the
        // actual mapping that is defined in the pipeline definition.
        final int parallelProcessors = 4; // 4 processors per member
        final int parallelOperations = 4; // 4 operations per processor
        final boolean preserveOrder = true;
        final String methodName = "doThingDotnet"; // the dotnet method to apply

        DotnetServiceConfig config = DotnetSubmit.getConfig(args)
                .withParallelism(parallelProcessors, parallelOperations)
                .withPreserveOrder(preserveOrder)
                .withMethodName(methodName);

        // create and define the pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline
                // source stage produces Entry<...> open generics
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                // dotnet transform produces an array of objects
                .apply(DotnetTransforms.mapRawAsync((service, input) -> {
                    DeserializingEntry entry = (DeserializingEntry) input;
                    Data[] rawInput = new Data[2];
                    rawInput[0] = entry.getDataKey();
                    rawInput[1] = entry.getDataValue();
                    return service.mapRawAsync(rawInput); // invoke dotnet
                }, config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that dotnet produces objects that are [0]:keyData and [1]:valueData
                .writeTo(Sinks.map("result-map", x -> ((Object[])x)[0], x -> ((Object[])x)[1]));

        // configure the job
        JobConfig jobConfig = new JobConfig();
        config.configureJob(jobConfig);

        // submit the job
        Hazelcast.bootstrappedInstance().getJet().newJob(pipeline, jobConfig);
    }
}
