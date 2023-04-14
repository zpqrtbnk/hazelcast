package com.hazelcast.jet.dotnet;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.concurrent.CompletableFuture;

// provides the dotnet transformations
public final class DotnetTransforms {

    private DotnetTransforms() { }

    // maps using dotnet
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

    // maps using dotnet
    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapRawAsync(
            @Nonnull BiFunctionEx<DotnetService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn,
            @Nonnull DotnetServiceConfig config) {

        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)),
                        DotnetService::destroy);

        // withAttachedDirectory "attaches a directory to this service factory under the given ID. It will
        // become a part of the Jet job and available to createContextFn() as processorContext.attachedDirectory(id)"
        // withAttachedFile is same but .attachedFile(id)
        if (config.hasDirectory()) {
            dotnetService.withAttachedDirectory(config.getDirectoryId(), new File(config.getDirectory()));
        }
        else {
            dotnetService.withAttachedFile(config.getDotnetExeId(), new File(config.getDotnetExe()));
        }

        return s -> s
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, mapAsyncFn)
                .setName(config.getMethodName());
    }
}
