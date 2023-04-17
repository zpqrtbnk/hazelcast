package com.hazelcast.jet.dotnet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Example {

    public static void main(String[] args) {

        final int parallelProcessors = 4; // 4 processors per member
        final int parallelOperations = 4; // 4 operations per processor
        final boolean preserveOrder = true;
        final String methodName = "doThingDotnet"; // the dotnet method to apply

        // fixme: should SUBMIT and configure with
        //  one directory containing win-x64, linux-x64, etc
        //  one filename base, eg dotjet, and on windows it's dotjet.exe (or we even look it up)
        //  the directory can contain only the file, or more
        // then, the ID for these should be PER JOB or better....?
        // ALSO the pipeName should be PER instance, PER JOB eg pipe-<instance-id>-<job-id>
        // 'cos one instance is going to run multiple job, in multiple directories
        // or maybe even multiple instance of the same dotnet exe
        // need to test whether the directory is recreated with the job's path or?

        DotnetServiceConfig config = DotnetSubmit.getConfig(args)
                .withParallelism(parallelProcessors, parallelOperations)
                .withPreserveOrder(preserveOrder)
                .withMethodName(methodName);

        // create and define the pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.mapJournal("streamed-map", JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()

                .apply(DotnetTransforms.mapAsync(Example::mapAsync, config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                .writeTo(Sinks.map("result-map"));

                /*
                .apply(DotnetTransforms.mapRawAsync(Example::mapRawAsync, config))
                .setLocalParallelism(config.getLocalParallelism()) // number of processors per member

                // we know that dotnet produces objects that are [0]:keyData and [1]:valueData
                //.writeTo(Sinks.map("result-map", x -> x[0], x -> x[1]));
                .writeTo(Sinks.map("result-map", Example::getItem0, Example::getItem1));
                //.writeTo(Sinks.map("result-map", x -> ((Object[])x)[0], x -> ((Object[])x)[1]));
                */

        // configure and submit the job
        JobConfig jobConfig = new JobConfig()
                // that JAR must go there, either because we include the Example class, or explicitly
                //.addJar(".../hazelcast-jet-dotnet-5.3.0-SNAPSHOT.jar")
                .addClass(Example.class);
        config.configureJob(jobConfig);
        Hazelcast.bootstrappedInstance().getJet().newJob(pipeline, jobConfig);
    }

//    private static FunctionEx<StreamStage<Map.Entry<Object,Object>>, StreamStage<Map.Entry<Object,Object>>> testMap0(DotnetServiceConfig config) {
//        return s -> s.apply(x -> x);
//    }

//    private static FunctionEx<StreamStage<Map.Entry<Object,Object>>, StreamStage<Map.Entry<Object,Object>>> testMap1x(DotnetServiceConfig config) {
//        // TODO: introduce deserializing entry
//        // then, service and all... until it fails and we know why
//        return s -> s.apply(x -> x);
//    }
//
//    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> testMap1(
//            @Nonnull FunctionEx< ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
//            @Nonnull DotnetServiceConfig config) {
//        return s -> s.apply(x -> (StreamStage<TResult>) x);
//    }
//
//    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> testMap2(
//            @Nonnull BiFunctionEx<DotnetService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
//            @Nonnull DotnetServiceConfig config) {
//        return s -> s.apply(x -> (StreamStage<TResult>) x);
//    }

//    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> testMap3(
//            @Nonnull BiFunctionEx<DotnetAltService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
//            @Nonnull DotnetServiceConfig config) {
//
//        final int maxConcurrentOps = config.getMaxConcurrentOps();
//        final boolean preserveOrder = config.getPreserveOrder();
//
//        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."
//
//        // let's remove that whole thing and see if *this* function that it wants to serialize, can be
//        /*
//        ServiceFactory<?, DotnetAltService> dotnetServiceFactory = ServiceFactories
//                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
//                // will create just one instance on each member and share it among the parallel task-lets."
//                .sharedService(
//                        processorContext -> new DotnetAltService(new DotnetServiceContext(processorContext, config)),
//                        dotnetService -> dotnetService.destroy());
//
//        // withAttachedDirectory "attaches a directory to this service factory under the given ID. It will
//        // become a part of the Jet job and available to createContextFn() as processorContext.attachedDirectory(id)"
//        // withAttachedFile is same but .attachedFile(id)
//        if (config.hasDirectory()) {
//            dotnetServiceFactory.withAttachedDirectory(config.getDirectoryId(), new File(config.getDirectory()));
//        }
//        else {
//            dotnetServiceFactory.withAttachedFile(config.getDotnetExeId(), new File(config.getDotnetExe()));
//        }
//        */
//
//        // still works
//        //return s -> s.apply(x -> (StreamStage<TResult>) x);
//
//        /*
//        // block below breaks
//        return s -> s
//                // this breaks
//                //.mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, mapAsyncFn)
//                // this breaks too
//                //.mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, Example::<TInput,TResult>serviceMap4)
//                // this... immediately dies due to null pointer
//                .mapUsingServiceAsync(null, maxConcurrentOps, preserveOrder, mapAsyncFn)
//                .setName(config.getMethodName());
//        */
//
//        // this does not work, still capturing a lambda somehow
//        //return s -> meh(s, dotnetServiceFactory, maxConcurrentOps, preserveOrder, config);
//
//        // dont see how this could work, it's going to capture the dotnet service (factory) again
//        // python is NOT using a shared service - it's using a different factory?
//        // and what-if ... we had a static DotnetService.factory(config) that returns a shared thing?
//        // so the thing below looks more like the python example BUT have no idea whether it's ok
//
//        // can this work AND still use a shared thing?
//        // no, that's not better, and the fucker will NEVER tell which lambda is the cause :(
//        //return s-> meh(s, Example.DotnetServiceFactory(config), maxConcurrentOps, preserveOrder, config);
//
//        // what if the factory does not actually references the concrete DotnetService class?
//        // nope, not getting any better, and arg!
//        // fails if dotnetServiceFactory produces IDotnetService
//        // if it produces a object-based thing? ie Data not serializable?
//        // no, not getting any better here :(
//        //return s-> mehAlt(s, dotnetServiceFactory, maxConcurrentOps, preserveOrder, config);
//        // no, that does not work either and WHAT THE FUCK REALLY
//        // with factory2 we're purely identical to python?
//        // and sure enough it still fails?!
//
//        // ok this is pretty fucked up now as this is the very same thing as python does, iic?
//        return s-> mehAlt(s, Example.DotnetAltServiceFactory2(config), maxConcurrentOps, preserveOrder, config);
//    }

//    public static ServiceFactory<?, DotnetService> DotnetServiceFactory(DotnetServiceConfig config) {
//        return ServiceFactories
//                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
//                // will create just one instance on each member and share it among the parallel task-lets."
//                .sharedService(
//                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
//                        DotnetService::destroy);
//    }
//
//    public static ServiceFactory<?, DotnetAltService> DotnetAltServiceFactory(DotnetServiceConfig config) {
//        return ServiceFactories
//                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
//                // will create just one instance on each member and share it among the parallel task-lets."
//                .sharedService(
//                        Example::createDotnetAltService, // but, config?
//                        DotnetAltService::destroy);
//    }

//    @Nonnull
//    static ServiceFactory<?, DotnetAltService> DotnetAltServiceFactory2(@Nonnull DotnetServiceConfig cfg) {
//        ServiceFactory<DotnetServiceContext, DotnetAltService> fac = ServiceFactory
//                .withCreateContextFn(ctx -> new DotnetServiceContext(ctx, cfg))
//                //.withDestroyContextFn(DotnetServiceContext::destroy)
//                .withCreateServiceFn((procCtx, serviceCtx) -> new DotnetAltService(serviceCtx))
//                .withDestroyServiceFn(DotnetAltService::destroy);
//        /*
//        if (cfg.baseDir() != null) {
//            File baseDir = Objects.requireNonNull(cfg.baseDir());
//            return fac.withAttachedDirectory(baseDir.toString(), baseDir);
//        } else {
//            File handlerFile = Objects.requireNonNull(cfg.handlerFile());
//            return fac.withAttachedFile(handlerFile.toString(), handlerFile);
//        }
//        */
//        return fac;
//    }

//    public static DotnetAltService createDotnetAltService(ProcessorSupplier.Context processorContext) {
//        try{
//            return new DotnetAltService(new DotnetServiceContext(processorContext, null));
//        } catch (Exception e) { return null; } // yea
//    }
//
//    public static <TInput, TResult> StreamStage<TResult> meh(StreamStage<TInput> inputStage,
//                                                             ServiceFactory<?, IDotnetService> dotnetService,
//                                                             int maxConcurrentOps,
//                                                             boolean preserveOrder,
//                                                             DotnetServiceConfig config) {
//        return inputStage
//                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, Example::<TInput,TResult>serviceMap4)
//                .setName(config.getMethodName());
//    }

//    public static <TInput, TResult> StreamStage<TResult> mehAlt(StreamStage<TInput> inputStage,
//                                                             ServiceFactory<?, DotnetAltService> dotnetService,
//                                                             int maxConcurrentOps,
//                                                             boolean preserveOrder,
//                                                             DotnetServiceConfig config) {
//        return inputStage
//                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, Example::<TInput,TResult>serviceMap4alt)
//                .setName(config.getMethodName());
//    }

//    public static <T> CompletableFuture<T> serviceMap1(T input) {
//        return CompletableFuture.completedFuture(input);
//    }
//
//    public static <T> CompletableFuture<T> serviceMap2(DotnetService service, T input) {
//        return CompletableFuture.completedFuture(input);
//    }
//
//    public static <T> CompletableFuture<T> serviceMap3(DotnetService service, T input) {
//        DeserializingEntry entry = (DeserializingEntry) input;
//        Data[] rawInput = new Data[2];
//        rawInput[0] = entry.getDataKey();
//        rawInput[1] = entry.getDataValue();
//        return CompletableFuture.completedFuture(input);
//    }
//
//    public static <TInput, TResult> CompletableFuture<TResult> serviceMap4(IDotnetService service, TInput input) {
//        DeserializingEntry entry = (DeserializingEntry) input;
//        Data[] rawInput = new Data[2];
//        rawInput[0] = entry.getDataKey();
//        rawInput[1] = entry.getDataValue();
//        return CompletableFuture.completedFuture((TResult) input);
//    }

//    public static <TInput, TResult> CompletableFuture<TResult> serviceMap4alt(DotnetAltService service, TInput input) {
//        DeserializingEntry entry = (DeserializingEntry) input;
//        Data[] rawInput = new Data[2];
//        rawInput[0] = entry.getDataKey();
//        rawInput[1] = entry.getDataValue();
//        return CompletableFuture.completedFuture((TResult) input);
//    }

//    private static CompletableFuture<Data[]> mapRawAsync(DotnetService service, Map.Entry<Object, Object> input) {
//        DeserializingEntry entry = (DeserializingEntry) input;
//        Data[] rawInput = new Data[2];
//        rawInput[0] = entry.getDataKey();
//        rawInput[1] = entry.getDataValue();
//        return service.mapRawAsync(rawInput); // invoke dotnet
//    }
//
    private static CompletableFuture<Map.Entry<Object, Object>> mapAsync(DotnetService service, Map.Entry<Object, Object> input) {

        // prepare input
        DeserializingEntry entry = (DeserializingEntry) input;
        Data[] rawInput = new Data[2];
        rawInput[0] = DeserializingEntryExtensions.getDataKey(entry);
        rawInput[1] = DeserializingEntryExtensions.getDataValue(entry);

        return service
                .mapAsync(rawInput) // invoke dotnet
                .thenApply(x -> DeserializingEntryExtensions.createNew(entry, x[0], x[1])); // map result
    }
//
//    private static Data getItem0(Data[] data) {
//        return data[0];
//    }
//
//    private static Data getItem1(Data[] data) {
//        return data[1];
//    }
}
