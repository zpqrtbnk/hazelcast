package com.hazelcast.jet.dotnet;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

// provides the dotnet transformations
public final class DotnetTransforms {

    private DotnetTransforms() { }

    // FIXME temp test code that will be removed, eventually
    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync0(@Nonnull DotnetServiceConfig config) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        return s -> s
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, DotnetService::<TInput, TResult>mapAsync0)
                .setName(config.getTransformName());
    }

    /*
    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync1(
            @Nonnull DotnetServiceConfig config,
            BiFunctionEx mapAsyncFn) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        return s -> s
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, mapAsyncFn)
                .setName(config.getMethodName());
    }
    */

    // maps using dotnet
    @Nonnull
    public static <TK1, TV1, TK2, TV2> FunctionEx<StreamStage<Map.Entry<TK1, TV1>>, StreamStage<Map.Entry<TK2, TV2>>> mapAsync(
            @Nonnull DotnetServiceConfig config) {

        return mapAsync(DotnetService::mapAsync, config);
    }

    // maps using dotnet
    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync(
            @Nonnull BiFunctionEx<DotnetService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
            @Nonnull DotnetServiceConfig config) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."

        ServiceFactory<?, DotnetService> serviceFactory = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        config.configureServiceFactory(serviceFactory);

        return s -> s
                .mapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder, mapAsyncFn)
                .setName(config.getTransformName());
    }
}
