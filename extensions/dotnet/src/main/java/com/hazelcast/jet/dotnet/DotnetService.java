package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.oop.DeserializingEntryExtensions;
import com.hazelcast.oop.channel.IJetPipe;
import com.hazelcast.oop.JetMessage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

// provides the dotnet service
public final class DotnetService {

    private final DotnetServiceContext serviceContext;
    private final DotnetHub dotnetHub;
    private final String transformName;

    // initializes a new dotnet service
    DotnetService(DotnetServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;
        this.dotnetHub = new DotnetHub(serviceContext);

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().fine("DotnetService created for " + instanceName);

        transformName = serviceContext.getConfig().getTransformName();
    }

    // FIXME temp test code that will be removed, eventually
    <TInput, TResult> CompletableFuture<TResult> mapAsync0(TInput input) {

        DotnetServiceConfig config = serviceContext.getConfig();
        String methodName = config.getTransformName();

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

    // maps an entry using dotnet
    public <TK1, TV1, TK2, TV2> CompletableFuture<Map.Entry<TK2, TV2>> mapAsync(Map.Entry<TK1, TV1> entry) {

        DeserializingEntry<TK1, TV1> deserializingEntry = (DeserializingEntry<TK1, TV1>) entry;
        Data[] data = new Data[2];
        data[0] = DeserializingEntryExtensions.getDataKey(deserializingEntry);
        data[1] = DeserializingEntryExtensions.getDataValue(deserializingEntry);
        return mapAsync(data)
                .thenApply(x -> DeserializingEntryExtensions.createNew(deserializingEntry, x[0], x[1]));
    }

    // maps using dotnet
    public CompletableFuture<Data[]> mapAsync(Data[] input) {
        IJetPipe pipe = dotnetHub.getPipe(); // cannot be null, hub would throw

        byte[][] requestBuffers = new byte[input.length][];
        for (int i = 0; i < input.length; i++) requestBuffers[i] = input[i].toByteArray();
        // FIXME assign operation IDs
        JetMessage requestMessage = new JetMessage(transformName, 0, requestBuffers);

        // FIXME we are missing some error-handling cases
        //   must ensure we always return the pipe to the hub, no matter what
        //   and, properly report exceptions that would occur in the futures

        // TODO: make it async so we can have fewer pipes than //
//        return dotnetHub.getPipeX()
//                .thenCompose(pipe -> pipe.write(requestMessage))
//                .thenCompose(x -> pipe.read()) // should pass the pipe along for returning it + handle errors
//                .thenApply(responseMessage -> null);

        return pipe
                .write(requestMessage)
                .thenCompose(x -> pipe.read())
                .thenApply(responseMessage -> {
                    dotnetHub.returnPipe(pipe);
                    serviceContext.getLogger().fine("T='" + responseMessage.getTransformName() + "'"); // FIXME temp
                    byte[][] responseBuffers = responseMessage.getBuffers();
                    Data[] data = new Data[responseBuffers.length];
                    for (int i = 0; i < data.length; i++) data[i] = new HeapData(responseBuffers[i]);
                    if (responseMessage.isException()) {
                        throw new CompletionException(new Exception("Remote call failed, check log."));
                    }
                    return data;
                });
    }

    public void destroy() {

        if (dotnetHub != null) dotnetHub.destroy();
    }
}
