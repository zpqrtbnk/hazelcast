package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.Logger;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.CompletableFuture;

public class TransformDoThing {

    public static <TInput, TResult> CompletableFuture<TResult> mapAsync(TInput input, DotnetServiceContext context, DotnetHub dotnetHub) {

        try {
            AsynchronousFileChannel channel = dotnetHub.getChannel();

            if (channel == null) {
                // FIXME what shall we do if we cannot proceed?
                context.getLogger().severe("err: no channel");
                return CompletableFuture.completedFuture(null);
            }

            // send the input, assume it's a serialized entry
            // TODO: but we need to support different type of inputs?
            // TODO: and, does it depend on the map BINARY or OBJECT storage?
            context.getLogger().info("Send input " + input.getClass().toString() + " value " + input);

            // we *could* serialize the input but that would allocate more buffers
            // assuming it's a map, we can pass the key/value pair instead?
            DeserializingEntry entry = (DeserializingEntry) input;
            ChannelExtensions.writeData(channel, entry.getDataKey());
            ChannelExtensions.writeData(channel, entry.getDataValue());

            // FIXME and now we need to read the result
            // say dotnet returns an array of objects
            // so: an int that contains the number of objects, and then each object

            return new ChannelObjectsReader(dotnetHub, channel).read();
            //return (CompletableFuture<TResult>) CompletableFuture.completedFuture(input);
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
