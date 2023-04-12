package com.hazelcast.jet.dotnet;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

// provides the dotnet service
public final class DotnetService {

    private final DotnetServiceContext serviceContext;
    private final DotnetHub dotnetHub;

    // initializes a new dotnet service
    DotnetService(DotnetServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;

        this.dotnetHub = serviceContext.getConfig().getMethodName().contains("Java")
            ? null
            : new DotnetHub(serviceContext);

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("DotnetService created for " + instanceName);
    }

    // maps, using the dotnet service
    <TInput, TResult> CompletableFuture<TResult> mapAsync(TInput input) {

        DotnetServiceConfig config = serviceContext.getConfig();
        String methodName = config.getMethodName();

        // FIXME: do this differently - maybe the method should be part of the config?
        // but... once we stop experimenting, we'd have only method? doThingDotnet?
        // except that method accepts a map entry and produces a map entry of sort,
        // could we have methods accepting... simple objects?!

        if (methodName.equals("toStringJava"))
            return (CompletableFuture<TResult>) Transforms.toStringJava((int) input, serviceContext);
        if (methodName.equals("toStringDotnet"))
            return (CompletableFuture<TResult>) Transforms.toStringDotnet((int) input, serviceContext, dotnetHub);
        if (methodName.equals("doThingJava"))
            return Transforms.doThingJava(input, serviceContext);
        if (methodName.equals("doThingDotnet"))
            return Transforms.doThingDotnet(input, serviceContext, dotnetHub);

        throw new UnsupportedOperationException("DotnetService does not support method '" + methodName + "'");
    }

    void destroy() {

        if (dotnetHub != null) dotnetHub.destroy();
    }
}
