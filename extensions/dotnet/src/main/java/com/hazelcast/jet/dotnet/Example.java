package com.hazelcast.jet.dotnet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

public class Example {

    private static String DOTNET_PATH = "c:\\Users\\sgay\\Code\\hazelcast-csharp-client\\src\\Hazelcast.Net.Jet\\bin\\Release\\net7.0\\win-x64\\publish\\win-x64";

    // this class needs to be declared as the main-class in the pom.xml file
    //
    // ../mvn/apache-maven-3.8.1/bin/mvn package -DskipTests -Dcheckstyle.skip=true
    // (and with the animal stuff disabled in pom.xml)
    //
    // #mkdir distribution/target/bin
    // #cp distribution/src/bin-*/* distribution/target/bin
    // #cp distribution/src/root/config/jvm-client.options distribution/target/config # or wherever HOME is?
    // (cd distribution/target && unzip hazelcast-5.3.0-SNAPSHOT.zip)
    // PATCH common.sh
    // alias hz-cli="distribution/target/hazelcast-5.3.0-SNAPSHOT/bin/hz-cli"
    //
    // hz-cli -v -t=dev@192.168.1.200:5701 submit java/out/artifacts/tutorial_python/tutorial-python.jar
    // hz-cli -v -t=dev@127.0.0.1:5701 submit extensions/dotnet/target/hazelcast-jet-dotnet-5.3.0-SNAPSHOT.jar
    // hz-cli -v -t=dev@127.0.0.1:5701 list-jobs

    public static void main(String[] args) {

        // MUST use the Windows directory here, the directory that actually contains our dotnet stuff
        // and, we point to the self-containing exe = 1 file only is needed
        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetPath(DOTNET_PATH)
                .withDotnetExe("dotjet.exe")
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName("doThing");

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
