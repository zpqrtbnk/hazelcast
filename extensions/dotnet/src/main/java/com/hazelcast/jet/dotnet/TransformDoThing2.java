package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.util.concurrent.CompletableFuture;

public class TransformDoThing2 {

    public static <TInput, TResult> CompletableFuture<TResult> mapJavaAsync(TInput input, DotnetServiceContext context) {

        DeserializingEntry entry = (DeserializingEntry) input;

        Object[] objects = new Object[2];

        Object key = entry.getKey(); // force deserialization of the key
        Object value = entry.getValue(); // force deserialization of the value

        objects[0] = entry.getDataKey().toByteArray(); // dotnet is not re-serializing either
        //context.getProcessorContext().hazelcastInstance().ser;;; // how can I get the serialization service?
        objects[1] = entry.getDataValue().toByteArray(); // FIXME

        // FIXME how can we create and serialize a whatever other object?

        return CompletableFuture.completedFuture((TResult) objects);
    }

    public static <TInput, TResult> CompletableFuture<TResult> mapDotnetAsync(TInput input, DotnetServiceContext context, DotnetHub2 dotnetHub) {

        try {
            IJetPipe pipe = dotnetHub.getPipe();

            if (pipe == null) {
                // FIXME what shall we do if we cannot proceed?
                context.getLogger().severe("err: no pipe");
                return CompletableFuture.completedFuture(null);
            }

            // send the input, assume it's a serialized entry
            // TODO: but we need to support different type of inputs?
            // TODO: and, does it depend on the map BINARY or OBJECT storage?
            context.getLogger().info("Send input " + input.getClass().toString() + " value " + input);

            // we *could* serialize the input but that would allocate more buffers
            // assuming it's a map, we can pass the key/value pair instead?
            DeserializingEntry entry = (DeserializingEntry) input;

            byte[][] buffers = new byte[2][];
            buffers[0] = entry.getDataKey().toByteArray();
            buffers[1] = entry.getDataValue().toByteArray();
            JetMessage message = new JetMessage(0, buffers);

            return pipe
                    .write(message)
                    .thenCompose(x -> pipe.read())
                    .thenApply(responseMessage -> {
                        Data[] data = new Data[2];
                        // FIXME work directly on the received buffer?
                        data[0] = new HeapData(responseMessage.getBuffers()[0]);
                        data[1] = new HeapData(responseMessage.getBuffers()[1]);
                        dotnetHub.returnPipe(pipe);
                        return (TResult) data; // FIXME this is not exactly pretty
                    });
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
