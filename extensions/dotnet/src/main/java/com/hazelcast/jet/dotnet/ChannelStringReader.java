package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class ChannelStringReader {

    private final DotnetHub dotnetHub;
    private final AsynchronousFileChannel channel;
    private final CompletionHandler<Integer, ChannelStringReader> completionHandler;
    private ByteBuffer buffer;
    private CompletableFuture future;
    private int readCount;
    private int state;
    private int length;

    public ChannelStringReader(DotnetHub dotnetHub, AsynchronousFileChannel channel) {
        this.dotnetHub = dotnetHub;
        this.channel = channel;

        readCount = 0;
        state = 0;
        completionHandler = createCompletionHandler();
    }

    private static CompletionHandler<Integer, ChannelStringReader> createCompletionHandler() {

        return new CompletionHandler<Integer, ChannelStringReader>() {
            @Override
            public void completed(Integer count, ChannelStringReader reader) {
                reader.readCompleted(count);
            }
            @Override
            public void failed(Throwable exc, ChannelStringReader reader) {
                reader.readFailed(exc);
            }
        };
    }

    private void readCompleted(Integer count) {
        // read until we have enough bytes, re-read as long as we don't...
        if (count < 0) {
            // FIXME can this happen and mean the channel is gone? and then?
            future.complete("WTF");
        }
        else {
            readCount += count;
            //Logger.getLogger("ChannelReader").info("Received " + count + " -> " + readCount);
            switch (state) {
                case 0: // waiting for length
                    if (readCount < 2) readChannel(); // read more
                    else {
                        length = buffer.get(0) + (buffer.get(1) << 8);
                        //Logger.getLogger("ChannelReader").info("Received length from dotnet process: " + length);
                        buffer = ByteBuffer.allocate(length);
                        buffer.clear();
                        readCount = 0;
                        state = 1; // wait for string
                        readChannel(); // read more
                    }
                    break;
                case 1: // waiting for string
                    if (readCount < length) readChannel(); // read more
                    else {
                        buffer.flip();
                        String s = StandardCharsets.US_ASCII.decode(buffer).toString();
                        Logger.getLogger("ChannelReader").info("Received string from dotnet process: " + s);
                        dotnetHub.returnChannel(channel);
                        future.complete(s);
                    }
            }
        }
    }

    private void readFailed(Throwable exc) {
        // exc is the exception
        System.out.println("failed");
        dotnetHub.returnChannel(channel);
        future.completeExceptionally(exc);
    }

    public CompletableFuture read() {
        buffer = ByteBuffer.allocate(4); // start with buffer for length
        buffer.clear();
        future = new CompletableFuture<>();
        readChannel();
        return future;
    }

    private void readChannel() {
        channel.read(buffer, 0, this, completionHandler);
    }
}
