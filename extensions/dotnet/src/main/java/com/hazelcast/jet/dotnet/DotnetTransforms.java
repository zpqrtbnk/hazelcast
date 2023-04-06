package com.hazelcast.jet.dotnet;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

public final class DotnetTransforms {

    private DotnetTransforms() {
    }

    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapUsingDotnet(@Nonnull DotnetServiceConfig config) {
        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        // each processor is going to get its own service, which should have its own pipe
        // but there should only be 1 dotnet process per member, so the services should
        // share some sort of common "thing" and can we ...?

        // that CANNOT work because "create service fn" must be serializable, what a joke
        // and THAT is why the config is serializable, so we need another way to handle this

        // and then... if the hub is static, since the 2 members end up running locally, what a mess

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                .nonSharedService(processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)), DotnetService::destroy)
                .toNonCooperative();

        return s -> s
                //.mapUsingService(DotnetService.getFactory(config), DotnetService::<TInput, TResult>process)
                .mapUsingService(dotnetService, DotnetService::<TInput, TResult>process)
                .setName("mapUsingDotnet");
    }

    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapUsingDotnetAsync(@Nonnull DotnetServiceConfig config) {
        final int maxConcurrentOps = config.getMaxConcurrentOps();
        final boolean preserveOrder = config.getPreserveOrder();

        ServiceFactory<?, DotnetService> dotnetService = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel tasklets."
                .sharedService(processorContext -> new DotnetService(new DotnetServiceContext(processorContext, config)), DotnetService::destroy)
                // non-cooperative: "We also declared the service as "non-cooperative" because it makes
                // blocking HTTP calls. Failing to do this would have severe consequences for the performance
                // of not just your pipeline, but all the jobs running on the Hazelcast cluster."
                // - no idea what this means - and whether it's relevant for async services?! -
                //.toNonCooperative()
                // make sure we destroy the service at the end of the job
                //.withDestroyServiceFn(DotnetService::destroy)
                ;

        return s -> s
                // doh - using a getFactory() means it's going to create several services
                // what we want is 1 service that can be used concurrently!
                //.mapUsingServiceAsync(DotnetService.getFactory(config), maxConcurrentOps, preserveOrder, DotnetService::<TInput, TResult>process)
                // this should do it
                .mapUsingServiceAsync(dotnetService, maxConcurrentOps, preserveOrder, DotnetService::<TInput, TResult>processAsync)
                .setName("mapUsingDotnet");
    }
}
