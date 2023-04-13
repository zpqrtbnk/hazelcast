package com.hazelcast.jet.dotnet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

public class Example {

    // TODO: there should be a way to pass DOTNET_PATH as an argument

    public static final String dotnetConfiguration = "Release"; // Release | Debug

    // add \\publish or /publish directory at the end of the path to use the standalone executable (self-contained)
    private final static String dotnetPath = OsHelper.isWindows()
            ? "c:\\Users\\sgay\\Code\\hazelcast-csharp-client\\src\\Hazelcast.Net.Jet\\bin\\" + dotnetConfiguration + "\\net7.0\\win-x64"
            : "/home/sgay/shared/dotjet/hazelcast-csharp-client/src/Hazelcast.Net.Jet/bin/" + dotnetConfiguration + "/net7.0/linux-x64";

    private final static String dotnetExe = OsHelper.isWindows()
            ? "dotjet.exe"
            : "dotjet";

    public static void main(String[] args) throws Exception {

        // future: specify the dotnet path as an argument
        //if (args.length != 1) {
        //    throw new Exception("Missing dotnetPath argument.");
        //}
        //
        // String dotnetPath = args[0];

        // MUST use the Windows directory here, the directory that actually contains our dotnet stuff
        // and, we point to the self-containing exe = 1 file only is needed
        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetPath(dotnetPath)
                .withDotnetExe(dotnetExe)
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName("doThingDotnet");

        Pipeline pipeline = Pipeline.create();
        pipeline
                // source stage produces Entry<...> open generics
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                // dotnet transform produces an array of objects
                .apply(DotnetTransforms.mapAsync(config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that the objects are [0]:keyData and [1]:valueData
                .writeTo(Sinks.map("result-map", x -> ((Object[])x)[0], x -> ((Object[])x)[1]));

        JobConfig cfg = new JobConfig()
                .setName("dotnet-jet")
                // MUST attach the windows directory BUT with the proper identifier, so files are pushed to cluster too
                .attachDirectory(config.getDotnetPath(), config.getDotnetPath().toString().replace('\\', '/'))
                ;

        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline, cfg);
    }

}
