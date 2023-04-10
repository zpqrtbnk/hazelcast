package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

// is this how we properly and asynchronously read a channel?
// or is this going to create an insanely deep stack trace?

public class ChannelObjectsReader {

    private static AtomicInteger ids = new AtomicInteger(); // DEBUG
    private final int id; // DEBUG
    private final DotnetHub dotnetHub;
    private final AsynchronousFileChannel channel;
    private final CompletionHandler<Integer, ChannelObjectsReader> completionHandler;
    private ByteBuffer buffer;
    private CompletableFuture future;
    private volatile int readCount, objectCount, objectIndex, length, state;
    private Object[] objects;

    public ChannelObjectsReader(DotnetHub dotnetHub, AsynchronousFileChannel channel) {
        this.dotnetHub = dotnetHub;
        this.channel = channel;

        this.id = ids.incrementAndGet();

        readCount = 0;
        state = 0;
        completionHandler = createCompletionHandler();
    }

    private static CompletionHandler<Integer, ChannelObjectsReader> createCompletionHandler() {

        return new CompletionHandler<Integer, ChannelObjectsReader>() {
            @Override
            public void completed(Integer count, ChannelObjectsReader reader) {
                reader.readCompleted(count);
            }
            @Override
            public void failed(Throwable exc, ChannelObjectsReader reader) {
                reader.readFailed(exc);
            }
        };
    }

    private void readCompleted(Integer count) {
        if (count < 0) {
            // FIXME can this happen and mean the channel is gone? and then?
            future.complete(null);
        }
        else {
            int rc = readCount;
            readCount += count;
            // FIXME how can I see log entries such as Received 4 -> 0 ???
            // FIXME seeing 4 -> 8 is strange already but... 4 -> 0 ???
            Logger.getLogger("ChannelReader").info("[" + id + "] Received " + count + " : " + rc + " -> " + readCount
                    + " / " + buffer.capacity() + " (state = " + state + ")");
            switch (state) {
                case 0: // waiting for objects count
                    if (readCount < 4) readChannel(); // read more
                    else {
                        objectCount = buffer.get(0) + (buffer.get(1) << 8) + (buffer.get(2) << 16) + (buffer.get(3) << 24);
                        Logger.getLogger("ChannelReader").info("[" + id + "] Received dotnet object count = " + objectCount);
                        objects = new Object[objectCount];
                        objectIndex = 0;
                        // keep the 4-bytes buffer, will be used for first object length
                        buffer.clear();
                        readCount = 0;
                        state = 1; // wait for first object
                        readChannel(); // read more
                    }
                    break;
                case 1: // waiting for object length
                    if (readCount < 4) readChannel(); // read more
                    else {
                        length = buffer.get(0) + (buffer.get(1) << 8) + (buffer.get(2) << 16) + (buffer.get(3) << 24);
                        Logger.getLogger("ChannelReader").info("[" + id + "] Received dotnet object #" + objectIndex + " length = " + length);
                        buffer = ByteBuffer.allocate(length);
                        buffer.clear();
                        readCount = 0;
                        state = 2; // read object data
                        readChannel(); // read more
                    }
                    break;
                case 2: // reading object data
                    if (readCount < length) readChannel(); // read more
                    else {
                        buffer.flip();
                        objects[objectIndex] = new HeapData(buffer.array());
                        Logger.getLogger("ChannelReader").info("[" + id + "] Received dotnet object #" + objectIndex + " count = " + objectCount);
                        if (++objectIndex < objectCount) {
                            buffer = ByteBuffer.allocate(4); // for the integer, make sure we don't read more (optimize this!)
                            buffer.clear();
                            readCount = 0;
                            state = 1; // read next object
                            readChannel();
                        }
                        else {
                            // we are done
                            Logger.getLogger("ChannelReader").info("[" + id + "] Complete");
                            dotnetHub.returnChannel(channel);
                            future.complete(objects);
                        }
                    }
                    break;
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
        buffer = ByteBuffer.allocate(4); // start with buffer for objects count
        buffer.clear();
        future = new CompletableFuture<>();
        readChannel();
        return future;
    }

    // I MAY BE DOING THIS TOTALLY WRONG
    // as in, read KEEPS READING ?!

    private void readChannel() {
        Logger.getLogger("ChannelReader").info("[" + id + "] read channel...");
        // FIXME what's "position" when reading from a pipe?
        channel.read(buffer, 0, this, completionHandler);
    }
}
