package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

final class DotnetService {

    private final DotnetServiceContext serviceContext;
    private final DotnetHub dotnetHub;

    DotnetService(DotnetServiceContext serviceContext) {

        this.serviceContext = serviceContext;
        // TODO: remove this exclusion
        this.dotnetHub = serviceContext.getConfig().getMethodName().equals("toStringJava")
                ? null
                : new DotnetHub(serviceContext);

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("Created dotnet service for " + instanceName);
    }

    <TInput, TResult> CompletableFuture<TResult> mapAsync(TInput input) {

        DotnetServiceConfig config = serviceContext.getConfig();

        // TODO need only 1 method, merge with TransformDoThing
        if (config.getMethodName().equals("toStringJava"))
            return (CompletableFuture<TResult>) TransformToString.mapJavaAsync((int) input, serviceContext);
        if (config.getMethodName().equals("toStringDotnet"))
            return (CompletableFuture<TResult>) TransformToString.mapDotnetAsync((int) input, serviceContext, dotnetHub);
        if (config.getMethodName().equals("doThing"))
            return TransformDoThing.mapAsync(input, serviceContext, dotnetHub);

        // not too sure if this is OK, we should probably rather throw
        return (CompletableFuture<TResult>) CompletableFuture.completedFuture(null);
    }

    void destroy() {
        if (dotnetHub != null) dotnetHub.destroy();
    }
}
