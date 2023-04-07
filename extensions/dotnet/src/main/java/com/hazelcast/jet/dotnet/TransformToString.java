package com.hazelcast.jet.dotnet;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.CompletableFuture;

public class TransformToString {

    public static CompletableFuture<String> mapJavaAsync(int input, DotnetServiceContext context) {
        return CompletableFuture.completedFuture("__" + input + "__");
    }

    public static CompletableFuture<String> mapDotnetAsync(int input, DotnetServiceContext context, DotnetHub dotnetHub) {

        try {
            AsynchronousFileChannel channel = dotnetHub.getChannel();

            if (channel == null) {
                // FIXME what shall we do if we cannot proceed?
                context.getLogger().severe("err: no channel");
                return CompletableFuture.completedFuture(null);
            }

            // send the integer
            context.getLogger().info("Send " + input);
            ChannelExtensions.writeInteger(channel, input);

            // create a reader, and obtain a future representing the reader waiting for the result
            context.getLogger().info("Sent " + input + ", wait for answer...");
            return new ChannelStringReader(dotnetHub, channel).read();
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
