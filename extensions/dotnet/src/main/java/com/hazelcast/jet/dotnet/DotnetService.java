package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;

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

    // map raw
    CompletableFuture<Data[]> mapRawAsync(Data[] input) {
        try {
            IJetPipe pipe = dotnetHub.getPipe();

            if (pipe == null) {
                // FIXME what shall we do if we cannot proceed?
                serviceContext.getLogger().severe("err: no pipe");
                return CompletableFuture.completedFuture(null);
            }

            byte[][] requestBuffers = new byte[input.length][];
            for (int i = 0; i < input.length; i++) requestBuffers[i] = input[i].toByteArray();
            JetMessage requestMessage = new JetMessage(0, requestBuffers);

            return pipe
                    .write(requestMessage)
                    .thenCompose(x -> pipe.read())
                    .thenApply(responseMessage -> {
                        dotnetHub.returnPipe(pipe);
                        byte[][] responseBuffers = responseMessage.getBuffers();
                        Data[] data = new Data[responseBuffers.length];
                        for (int i = 0; i < data.length; i++) data[i] = new HeapData(responseBuffers[i]);
                        return data;
                    });
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }

    void destroy() {

        if (dotnetHub != null) dotnetHub.destroy();
    }
}
