package com.hazelcast.jet.dotnet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;

// TODO:
//   an example class that submits an example job
//   should not be part of the final module, of course

public class Example {

    public static void main(String[] args) {

        final int parallelProcessors = 4; // 4 processors per member
        final int parallelOperations = 4; // 4 operations per processor
        final boolean preserveOrder = true;
        final String methodName = "doThingDotnet"; // the dotnet method to apply

        // the method name is in case we want the dotnet process to support several
        // methods - there is always one process per job per member, anyway, but
        // we may want to re-use one unique executable for different jobs

        DotnetServiceConfig config = DotnetSubmit.getConfig(args)
                .withParallelism(parallelProcessors, parallelOperations)
                .withPreserveOrder(preserveOrder)
                .withTransformName(methodName);

        // create and define the pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                .apply(DotnetTransforms.mapAsync(config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                .writeTo(Sinks.map("result-map"));

        // configure and submit the job
        JobConfig jobConfig = new JobConfig()
                // that JAR must go there, either because we include the Example class, or explicitly
                //.addJar(".../hazelcast-jet-dotnet-5.3.0-SNAPSHOT.jar")
                .addClass(Example.class);
        config.configureJob(jobConfig);
        Hazelcast.bootstrappedInstance().getJet().newJob(pipeline, jobConfig);
    }
}
