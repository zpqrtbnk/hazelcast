package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class TransformToString2 {
    public static CompletableFuture<String> mapJavaAsync(int input, DotnetServiceContext context) {
        return CompletableFuture.completedFuture("__" + input + "__");
    }

    public static CompletableFuture<String> mapDotnetAsync(int input, DotnetServiceContext context, DotnetHub2 dotnetHub) {

        try {
            IJetPipe pipe = dotnetHub.getPipe();

            if (pipe == null) {
                // FIXME what shall we do if we cannot proceed?
                context.getLogger().severe("err: no pipe");
                return CompletableFuture.completedFuture(null);
            }

            byte[][] buffers = new byte[1][0];
            buffers[0] = new byte[4];
            int tmp = input;
            for (int i = 0; i < 4; i++) {
                buffers[0][i] = (byte)(tmp & 255);
                tmp >>= 8;
            }
            JetMessage message = new JetMessage(0, buffers);
            context.getLogger().info("Send " + input);

            return pipe
                    .write(message)
                    .thenCompose(x -> pipe.read())
                    .thenApply(responseMessage -> {
                        // FIXME maybe then, the message could expose the raw ByteBuffer slices we've received?
                        String s = StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(responseMessage.getBuffers()[0])).toString();
                        Logger.getLogger("Transform").info("Received string from dotnet process: " + s);
                        dotnetHub.returnPipe(pipe);
                        return s;
                    });
        }
        catch (Exception e) {
            // FIXME what shall we do if we cannot proceed?
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }
}
