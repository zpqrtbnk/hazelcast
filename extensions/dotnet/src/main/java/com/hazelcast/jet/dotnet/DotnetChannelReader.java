package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class DotnetChannelReader {

    private DotnetHub dotnetHub;
    private AsynchronousFileChannel channel;
    private ByteBuffer buffer;
    private CompletableFuture future;
    private int readCount;
    private int state;
    private int length;
    private CompletionHandler<Integer, DotnetChannelReader> completionHandler;

    public DotnetChannelReader(DotnetHub dotnetHub, AsynchronousFileChannel channel, ByteBuffer buffer, CompletableFuture future) {
        this.dotnetHub = dotnetHub;
        this.channel = channel;
        this.buffer = buffer;
        this.future = future;

        readCount = 0;
        state = 0;
        completionHandler = createCompletionHandler(); // could it be static?
    }

    public DotnetChannelReader(DotnetHub dotnetHub, AsynchronousFileChannel channel) {
        this.dotnetHub = dotnetHub;
        this.channel = channel;

        readCount = 0;
        state = 0;
        completionHandler = createCompletionHandler(); // could it be static?
    }

    private static CompletionHandler<Integer, DotnetChannelReader> createCompletionHandler() {

        return new CompletionHandler<Integer, DotnetChannelReader>() {
            @Override
            public void completed(Integer count, DotnetChannelReader reader) {
                reader.readCompleted(count);
            }
            @Override
            public void failed(Throwable exc, DotnetChannelReader reader) {
                reader.readFailed(exc);
            }
        };
    }

    private void readCompleted(Integer count) {
        // read until we have enough bytes, re-read as long as we don't...
        if (count < 0) {
            // is that even possible? and then, is it a failure?
            future.complete("WTF");
        }
        else {
            readCount += count;

            switch (state) {
                case 0: // waiting for length
                    if (readCount < 2) readChannel(); // read more
                    else {
                        length = buffer.get(0) + (buffer.get(1) << 8);
                        buffer = ByteBuffer.allocate(length);
                        buffer.clear();
                        readCount = 0;
                        state = 1; // wait for string
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
