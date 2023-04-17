package com.hazelcast.jet.dotnet;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

    the test performs correctly
    next:
    - check all the comments for things to fix and improve, it's pretty rough for now
    - split the dotnet code into (a) the service code and (b) the transform itself
    - figure out a fast binary serializer between Java and .NET
    - for larger payload investigate doing IPC with shared memory

    about serialization...
    - MessagePack?
    - do we want to "automagically" serialize things?
    - or ask users to provide serializers for the pipeline values?

 */
public class DotnetJetTest extends SimpleTestInClusterSupport {

    private static final int ITEM_COUNT = 10_000; // for strings
    private static final int THINGS_COUNT = 10_000; // for things

    public static final String dotnetConfiguration = "Release"; // Release | Debug

    // note: for tests, the EXE is NOT uploaded to the member and HAS to be in the specified path on the SAME MACHINE

    // add \\publish or /publish directory at the end of the path to use the standalone executable (self-contained)
    private final static String dotnetPath = OsHelper.isWindows()
            ? "c:\\Users\\sgay\\Code\\hazelcast-csharp-client\\src\\Hazelcast.Net.Jet\\bin\\" + dotnetConfiguration + "\\net7.0\\win-x64"
            : "/home/sgay/shared/dotjet/hazelcast-csharp-client/src/Hazelcast.Net.Jet/bin/" + dotnetConfiguration + "/net7.0/linux-x64";

    private final static String dotnetExe = OsHelper.isWindows()
            ? "dotjet.exe"
            : "dotjet";

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceWithResourceUploadConfig();

        // activate journal for map named 'streamed_map'
        MapConfig mapConfig = new MapConfig("streamed-map");
        mapConfig.getEventJournalConfig().setEnabled(true);
        config.addMapConfig(mapConfig);

        // explicitly register serializers for polyglot serialization
        config.getSerializationConfig().getCompactSerializationConfig().addSerializer(new SomeThingSerializer());
        config.getSerializationConfig().getCompactSerializationConfig().addSerializer(new OtherThingSerializer());

        initialize(2, config);
    }

    @Test
    public void toStringUsingJava() {
        toStringUsing("toStringJava");
    }

    @Test
    public void toStringUsingDotnet() {
        toStringUsing("toStringDotnet");
    }

    public void toStringUsing(String methodName) {

        long startTime = System.currentTimeMillis();

        List<Integer> items = IntStream
                .range(0, ITEM_COUNT)
                .boxed() // convert to Integer instances
                .collect(toList());

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(dotnetPath)
                .withDotnetExe(dotnetExe)
                // 4 processors per member, 4 operations per processor, the dotnet hub will open 16 channels
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName(methodName);

        // gather the expected results
        List<String> expected = items.stream().map(i -> "__" + i + "__").collect(toList());

        // compose the pipeline
        Pipeline p = Pipeline.create();
        p
                .readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0)

                .apply(DotnetTransforms.<Integer, String>mapAsync0(config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                .writeTo(AssertionSinks.assertAnyOrder("Fail to get expected items.", expected));

        // submit the job & wait for completion
        instance().getJet().newJob(p).join();

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println(totalTime);
    }

    @Test
    public void doThingUsingJava() {

        doThingUsing("doThingJava");
    }

    @Test
    public void doThingUsingDotnet() {
        doThingUsing("doThingDotnet");
    }

    public void doThingUsing(String methodName) {

        long startTime = System.currentTimeMillis();

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(dotnetPath)
                .withDotnetExe(dotnetExe)
                // 4 processors per member, 4 operations per processor, the dotnet hub will open 16 channels
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName(methodName);

        // we're going to work with a map journal source
        // the journal is activated in the member config (see top of this file)

        // compose the pipeline
        Pipeline p = Pipeline.create();
        p
                // source stage produces Entry<...> open generics
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                // dotnet transform produces an array of objects
                .apply(DotnetTransforms.mapAsync0(config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that the objects are [0]:keyData and [1]:valueData
                .writeTo(Sinks.map("result-map", x -> ((Object[])x)[0], x -> ((Object[])x)[1]));

        // troubleshooting:
        //.writeTo(Sinks.logger());

        // submit the job
        Job job = instance().getJet().newJob(p);

        // wait for the job to be actually running
        // else, whatever we put in the map will not be processed (?)
        JobStatus status = job.getStatus();
        while (status != JobStatus.RUNNING) {
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }
            status = job.getStatus();
        }

        // now anytime we add an entry to streamed-map, the job should produce a corresponding entry
        IMap sourceMap = instance().getMap("streamed-map");
        Logger.getLogger("TEST").info("set values in map...");
        sourceMap.clear();
        for (int i = 0; i < THINGS_COUNT; i++)
        {
            SomeThing thing = new SomeThing();
            thing.setValue(i);
            sourceMap.set("thing-" + i, thing);
        }
        Logger.getLogger("TEST").info("done setting values in map...");

        // wait, the result map is going to be populated by the job
        IMap resultMap = instance().getMap("result-map");
        int timeoutSeconds = 30;
        while (resultMap.size() < THINGS_COUNT && --timeoutSeconds > 0)
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }

        Assert.assertNotEquals(0, timeoutSeconds);

        // we get the values as DeserializingEntry which contain DATA for both key and value,
        // which we extract and pass directly to .NET (so, no duplicate de-serialization). and
        // .NET returns DATA too, which we pass to Java map.set which detects it's already
        // serialized (so, no duplicate serialization either).
        //
        // is this linked to how entries are kept BINARY vs OBJECT?

        // streaming jobs run until canceled, and then they end up in a failed state
        // try/catch the join 'cos cancellation is an exception, apparently
        job.cancel();
        try {
            job.join();
        }
        catch (Exception e) {
            Logger.getLogger("TEST").fine(e);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println(totalTime);
    }

    @Test
    public void doThingUsingDotnet2() {

        long startTime = System.currentTimeMillis();

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(dotnetPath)
                .withDotnetExe(dotnetExe)
                // 4 processors per member, 4 operations per processor, the dotnet hub will open 16 channels
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName("doThingDotnet");

        // we're going to work with a map journal source
        // the journal is activated in the member config (see top of this file)

        // compose the pipeline
        Pipeline p = Pipeline.create();
        p
                // source stage produces Entry<...> open generics
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                // dotnet transforms Data[] to Data[]
                .apply(DotnetTransforms.mapAsync((service, input) -> {
                    DeserializingEntry entry = (DeserializingEntry) input;
                    Data[] rawInput = new Data[2];
                    rawInput[0] = DeserializingEntryExtensions.getDataKey(entry);
                    rawInput[1] = DeserializingEntryExtensions.getDataValue(entry);
                    return service.mapAsync(rawInput);
                }, config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that the returned objects are [0]:keyData and [1]:valueData
                .writeTo(Sinks.map("result-map", x -> ((Data[])x)[0], x -> ((Data[])x)[1]));

        // submit the job
        Job job = instance().getJet().newJob(p);

        // wait for the job to be actually running
        // else, whatever we put in the map will not be processed (?)
        JobStatus status = job.getStatus();
        while (status != JobStatus.RUNNING) {
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }
            status = job.getStatus();
        }

        // now anytime we add an entry to streamed-map, the job should produce a corresponding entry
        IMap sourceMap = instance().getMap("streamed-map");
        Logger.getLogger("TEST").info("set values in map...");
        sourceMap.clear();
        for (int i = 0; i < THINGS_COUNT; i++)
        {
            SomeThing thing = new SomeThing();
            thing.setValue(i);
            sourceMap.set("thing-" + i, thing);
        }
        Logger.getLogger("TEST").info("done setting values in map...");

        // wait, the result map is going to be populated by the job
        IMap resultMap = instance().getMap("result-map");
        int timeoutSeconds = 30;
        while (resultMap.size() < THINGS_COUNT && --timeoutSeconds > 0)
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }

        Assert.assertNotEquals(0, timeoutSeconds);

        // this fails, because we don't have the schema for OtherThing
        resultMap.put("dummy", new OtherThing()); // unless we force the schema to exist
        Set<Map.Entry<String, OtherThing>> results = resultMap.entrySet();
        for (Map.Entry<String, OtherThing> entry : results)
            System.out.println(">> " + entry.getKey() + ": " + entry.getValue());

        // streaming jobs run until canceled, and then they end up in a failed state
        // try/catch the join 'cos cancellation is an exception, apparently
        job.cancel();
        try {
            job.join();
        }
        catch (Exception e) {
            Logger.getLogger("TEST").fine(e);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println(totalTime);
    }

    @Test
    public void doThingUsingJava2() {

        long startTime = System.currentTimeMillis();

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(dotnetPath)
                .withDotnetExe(dotnetExe)
                // 4 processors per member, 4 operations per processor, the dotnet hub will open 16 channels
                .withParallelism(4, 4)
                .withPreserveOrder(true)
                .withMethodName("doThingDotnet");

        // we're going to work with a map journal source
        // the journal is activated in the member config (see top of this file)

        // compose the pipeline
        Pipeline p = Pipeline.create();
        p
                // source stage produces Entry<...> open generics
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                // dotnet transforms Data[] to Data[]
                .apply(DotnetTransforms.mapAsync((service, input) -> {
                    DeserializingEntry entry = (DeserializingEntry) input;
                    SomeThing someThing = (SomeThing) entry.getValue();
                    Object[] output = new Object[2];
                    output[0] = DeserializingEntryExtensions.getDataKey(entry);
                    OtherThing otherThing = new OtherThing();
                    otherThing.setValue("__" + someThing.getValue() + "__");
                    output[1] = otherThing;
                    return CompletableFuture.completedFuture(output);
                }, config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that the returned objects are [0]:keyData and [1]:valueData
                .writeTo(Sinks.map("result-map", x -> ((Object[])x)[0], x -> ((Object[])x)[1]));

        // submit the job
        Job job = instance().getJet().newJob(p);

        // wait for the job to be actually running
        // else, whatever we put in the map will not be processed (?)
        JobStatus status = job.getStatus();
        while (status != JobStatus.RUNNING) {
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }
            status = job.getStatus();
        }

        // now anytime we add an entry to streamed-map, the job should produce a corresponding entry
        IMap sourceMap = instance().getMap("streamed-map");
        Logger.getLogger("TEST").info("set values in map...");
        sourceMap.clear();
        for (int i = 0; i < THINGS_COUNT; i++)
        {
            SomeThing thing = new SomeThing();
            thing.setValue(i);
            sourceMap.set("thing-" + i, thing);
        }
        Logger.getLogger("TEST").info("done setting values in map...");

        // wait, the result map is going to be populated by the job
        IMap resultMap = instance().getMap("result-map");
        int timeoutSeconds = 30;
        while (resultMap.size() < THINGS_COUNT && --timeoutSeconds > 0)
            try { Thread.sleep(1_000); } catch (InterruptedException e) { }

        Assert.assertNotEquals(0, timeoutSeconds);

        // this fails, because we don't have the schema for OtherThing
        resultMap.put("dummy", new OtherThing()); // unless we force the schema to exist
        Set<Map.Entry<String, OtherThing>> results = resultMap.entrySet();
        for (Map.Entry<String, OtherThing> entry : results)
            System.out.println(">> " + entry.getKey() + ": " + entry.getValue());

        // streaming jobs run until canceled, and then they end up in a failed state
        // try/catch the join 'cos cancellation is an exception, apparently
        job.cancel();
        try {
            job.join();
        }
        catch (Exception e) {
            Logger.getLogger("TEST").fine(e);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println(totalTime);
    }
}

