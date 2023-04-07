package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.Logger;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.CompletableFuture;

public class TransformDoThing {

    public static <TInput, TResult> CompletableFuture<TResult> mapAsync(TInput input, DotnetServiceContext context, DotnetHub dotnetHub) {
        // now how is this going to work?
        // we should have passed the unique method name to .NET so it knows what to expect + what to do
        // ie the process should receive the pipe name, the channels count, and the method name
        // but - could we want a process to be able to perform 2 transforms and then how? no.
        // then ... ?
        // we need a way to serialize TInput and TResult...
        //
        // and then in a pure-dotnet world if I put "Thing" objets into the map,
        // what kind of object is the map containing, Java-side? can we have access
        // to the binary payload of keys and values? alternatively, what's mapJournal
        // going to return?

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
            DeserializingEntry entry = (DeserializingEntry) input;
            ChannelExtensions.writeData(channel, entry.getDataKey());
            ChannelExtensions.writeData(channel, entry.getDataValue());

            // FIXME and now we need to read the result
            // would it be a single entry OR a key/value pair?
            // or maybe we need to support both? what will we do with the result?
            // put it back in a map... or ?
            // if it's a single object how can we deserialize it?
            // can we put it back in a map without deserializing it?

            //return new ChannelObjectReader(dotnetHub, channel).read();
            return (CompletableFuture<TResult>) CompletableFuture.completedFuture(input);
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
