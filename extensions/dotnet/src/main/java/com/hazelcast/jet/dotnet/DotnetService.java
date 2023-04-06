package com.hazelcast.jet.dotnet;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

final class DotnetService {

    private DotnetServiceContext serviceContext;
    private DotnetHub dotnetHub;

    DotnetService(DotnetServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        dotnetHub = DotnetHub.get(serviceContext);
        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("Created dotnet service for " + instanceName);
    }

    <TInput, TResult> TResult process(TInput input) {
        DotnetServiceConfig config = serviceContext.getConfig();

        if (config.getInputClass() == Integer.class && config.getOutputClass() == String.class) {
            if (config.getMethodName().equals("javaToString"))
                return (TResult) new ProcessJava().process((int) input, serviceContext);
        }

        return null;
    }

    <TInput, TResult> CompletableFuture<TResult> processAsync(TInput input) {

        DotnetServiceConfig config = serviceContext.getConfig();

        if (config.getInputClass() == Integer.class && config.getOutputClass() == String.class) {
            if (config.getMethodName().equals("javaToString"))
                return (CompletableFuture<TResult>) new ProcessJava().processAsync((int) input, serviceContext);
            if (config.getMethodName().equals("dotnetToStringAsync"))
                return (CompletableFuture<TResult>) ProcessDotnetAsync.processAsync((int) input, serviceContext, dotnetHub);
        }

        // not too sure if this is OK, we should probably rather throw
        return (CompletableFuture<TResult>) CompletableFuture.completedFuture(null);
    }

    void destroy() {
        dotnetHub.free();
    }
}
