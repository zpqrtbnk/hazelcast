package com.hazelcast.jet.dotnet;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.CompletableFuture;

public class ProcessDotnetAsync {

    public static CompletableFuture<String> processAsync(int input, DotnetServiceContext context, DotnetHub dotnetHub) {

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
            return new DotnetChannelReader(dotnetHub, channel).read();
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
