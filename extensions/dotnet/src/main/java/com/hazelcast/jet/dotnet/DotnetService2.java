package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

public class DotnetService2 {
    private final DotnetServiceContext serviceContext;
    private final DotnetHub2 dotnetHub;

    DotnetService2(DotnetServiceContext serviceContext) {

        this.serviceContext = serviceContext;
        // TODO: remove this exclusion
        this.dotnetHub = serviceContext.getConfig().getMethodName().equals("toStringJava")
                ? null
                : new DotnetHub2(serviceContext);

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("Created dotnet service for " + instanceName);
    }

    <TInput, TResult> CompletableFuture<TResult> mapAsync(TInput input) {

        DotnetServiceConfig config = serviceContext.getConfig();

        // TODO need only 1 method, merge with TransformDoThing
        if (config.getMethodName().equals("toStringJava"))
            return (CompletableFuture<TResult>) TransformToString2.mapJavaAsync((int) input, serviceContext);
        if (config.getMethodName().equals("toStringDotnet"))
            return (CompletableFuture<TResult>) TransformToString2.mapDotnetAsync((int) input, serviceContext, dotnetHub);
        if (config.getMethodName().equals("doThingJava"))
            return TransformDoThing2.mapJavaAsync(input, serviceContext);
        if (config.getMethodName().equals("doThing"))
            return TransformDoThing2.mapDotnetAsync(input, serviceContext, dotnetHub);

        // not too sure if this is OK, we should probably rather throw
        return (CompletableFuture<TResult>) CompletableFuture.completedFuture(null);
    }

    void destroy() {
        if (dotnetHub != null) dotnetHub.destroy();
    }
}
