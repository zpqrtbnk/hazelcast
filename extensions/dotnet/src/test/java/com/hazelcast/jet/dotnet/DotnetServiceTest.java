package com.hazelcast.jet.dotnet;

import com.hazelcast.config.Config;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/*

    NOTES

    the test runs 1 job, on 2 members
    so each member is going to run its own local job
    each member job is driven by setLocalParallelism which we configure to be 4
    so, each member job is going to fork 4 processors for the dotnet stage
    the processors are going to share one 'shared' service created by a factory
    the service is relying on a 'hub' that runs, controls, and connects to the dotnet process
    (the hub comes from a time I thought we'd have non-shared services, sharing a hub)
    (but now we can probably merge the service and the hub together)
    the hub/service is therefore supporting 4 processors
    and each processor is configured with maxConcurrentOps being 4, meaning they can
    send 4 ops at a time (concurrently) to the service => the hub/service must be
    able to serve 16 concurrent operations, and therefore will start a dotnet process
    that has 16 listening tasks, and will open 16 corresponding asynchronous channels

    the processor fires 4 operations, and for each operation the service returns
    a future. when the processor wants to fire more operations, it checks the futures
    to see if one has completed. so... new operations are not driven by previous
    operations completing, but by Jet.

 */
public class DotnetServiceTest extends SimpleTestInClusterSupport {

    private static final int ITEM_COUNT = 10_000;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceWithResourceUploadConfig();
        initialize(2, config);
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingDotnet() {

        List<Integer> items = IntStream
                .range(0, ITEM_COUNT)
                //.mapToObj(Integer::toString) // convert to String instances
                .boxed() // convert to Integer instances
                .collect(toList());

        DotnetServiceConfig config = new DotnetServiceConfig()

                // using the self-contained exe means that .NET does not need to be deployed on the host
                .withDotnetPath("c:\\Users\\sgay\\Code\\dotnet-jet\\dotnet-svr")
                //.withDotnetExe("bin\\debug\\net7.0\\win-x64\\dotnet-svr.exe") // normal exe
                .withDotnetExe("bin\\Release\\net7.0\\win-x64\\publish\\win-x64\\dotnet-svr.exe") // self-contained

                // 4 processors, each supporting 4 concurrent ops = the dotnet hub will open 16 channels
                .withMaxConcurrentOps(4) // number of concurrent ops per processor
                .withLocalParallelism(4) // number of processors

                .withPreserveOrder(true)
                .withPipeName("dotnet-jet") // passed to dotnet project, names the pipe
                .withInputClass(Integer.class)
                .withOutputClass(String.class)

                //.withMethodName("javaToString"); // plain Java
                .withMethodName("dotnetToStringAsync"); // asynchronous named pipe IPC to dotnet

        // gather the expected results
        List<String> expected = items.stream().map(i -> "__" + i + "__").collect(toList());

        // compose the pipeline
        Pipeline p = Pipeline.create();
        p
                .readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0)
                .apply(DotnetTransforms.<Integer, String>mapUsingDotnetAsync(config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member
                .writeTo(AssertionSinks.assertAnyOrder("Dotnet didn't map the items correctly", expected));

        // submit the job
        instance().getJet().newJob(p).join();
    }
}
