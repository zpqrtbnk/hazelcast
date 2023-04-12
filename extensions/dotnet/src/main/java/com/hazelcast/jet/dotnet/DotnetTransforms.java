package com.hazelcast.jet.dotnet;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

public final class DotnetTransforms {

    private DotnetTransforms() { }

    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync(@Nonnull DotnetServiceConfig config) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        return s -> s
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, DotnetService::<TInput, TResult>mapAsync)
                .setName(config.getMethodName());
    }

    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync2(@Nonnull DotnetServiceConfig config) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        ServiceFactory<?, DotnetService2> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService2(new DotnetServiceContext(processorContext, config)),
                        DotnetService2::destroy);

        return s -> s
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, DotnetService2::<TInput, TResult>mapAsync)
                .setName(config.getMethodName());
    }
}
