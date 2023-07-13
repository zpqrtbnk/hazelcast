package com.hazelcast.jet.dotnet;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.yaml.*;
import com.hazelcast.oop.DeserializingEntryExtensions;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class DotnetStepProvider implements StepProvider {

    @Override
    public SourceStep[] getSources() {
        return null;
    }

    @Override
    public TransformStep[] getTransforms() {
        return new TransformStep[] {
            new TransformStep("dotnet", DotnetStepProvider::transformDotnet)
        };
    }

    @Override
    public SinkStep[] getSinks() {
        return null;
    }

    // - transform: dotnet
    //   dotnet-dir:
    //   dotnet-exe: name of the executable (without .exe, we'll figure it out)
    //   dotnet-method: name of the service method
    //   preserve-order: true|false
    //   parallelism:
    //     processors: number of processors (jet local parallelism)
    //     operations: number of operations (dotnet service concurrent ops)
    private static Object transformDotnet(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        DotnetServiceConfig config = new DotnetServiceConfig()
                .withDotnetDir(YamlUtils.getProperty(properties, "dotnet-dir"))
                .withDotnetExe(YamlUtils.getProperty(properties, "dotnet-exe"))
                .withTransformName(YamlUtils.getProperty(properties,"dotnet-method"))
                .withPreserveOrder(YamlUtils.getProperty(properties, "preserve-order", true));

        YamlMapping parallelism = properties.childAsMapping("parallelism");
        if (parallelism != null) {
            config.withParallelism(
                    YamlUtils.getProperty(parallelism, "processors"),
                    YamlUtils.getProperty(parallelism, "operations"));
        }

        // FIXME this, stageContext, timestamps should already have been taken care of
        StreamStage streamStage = stageContext instanceof StreamStage ? (StreamStage) stageContext :
                //stageContext instanceof StreamSourceStage ? ((StreamSourceStage) stageContext).withoutTimestamps() :
                null;

        if (streamStage == null)
            throw new JobBuilderException("panic: unsupported stage type");

//        return streamStage
//                .apply(mapAsync((service, input) -> {
//                    DeserializingEntry entry = (DeserializingEntry) input;
//                    Data[] data = new Data[2];
//                    data[0] = DeserializingEntryExtensions.getDataKey(entry);
//                    data[1] = DeserializingEntryExtensions.getDataValue(entry);
//                    return service
//                            .mapAsync(data)
//                            .thenApply(x -> DeserializingEntryExtensions.createNew(entry, x[0], x[1]));
//                }, config))
//                .setLocalParallelism(config.getLocalParallelism());

        // alt:

        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."
        //
        // "On each member Jet calls createContextFn() to get a context object that will be shared across
        // all the service instances on that member. For example, if you are connecting to an external
        // service that provides a thread-safe client, you can create it here and then create individual
        // sessions for each service instance."
        //
        // "Jet repeatedly calls {@link #createServiceFn()} to create as many service instances on each
        // member as determined by the localParallelism of the pipeline stage. The invocations of
        // createServiceFn() receive the context object.
        //
        // "When the job is done, Jet calls destroyServiceFn() with each service instance. Finally, Jet
        // calls destroyContextFn() with the context object."

        ServiceFactory<?, DotnetService> serviceFactory = ServiceFactories

                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                //
                // here, createContextFn (which creates the thing that is shared) creates the service
                // itself, and createServiceFn (which creates each service instance) just passes the
                // service along.

                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        config.configureServiceFactory(serviceFactory);

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        // can we avoid the 'apply' indirection and map directly here?
        return streamStage
                .mapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder, (service, input) -> {
                    // FIXME what is DATA-IN and DATA-OUT? tuples vs entries?
                    DeserializingEntry entry = (DeserializingEntry) input;
                    Data[] data = DeserializingEntryExtensions.getData(entry);
                    return ((DotnetService) service)
                            .mapAsync(data)
                            .thenApply(x -> DeserializingEntryExtensions.createNew(entry, x));
                })
                .setName(config.getTransformName());
    }

//    @Nonnull
//    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync(
//            @Nonnull BiFunctionEx<DotnetService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
//            @Nonnull DotnetServiceConfig config) {
//
//        final int maxConcurrentOps = config.getMaxConcurrentOps();
//        final boolean preserveOrder = config.getPreserveOrder();
//
//        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."
//
//        ServiceFactory<?, DotnetService> serviceFactory = ServiceFactories
//                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
//                // will create just one instance on each member and share it among the parallel task-lets."
//                .sharedService(
//                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
//                        DotnetService::destroy);
//
//        config.configureServiceFactory(serviceFactory);
//
//        return s -> s
//                .mapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder, mapAsyncFn)
//                .setName(config.getTransformName());
//    }
}
