package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.serialization.Data;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.ExecutionException;

public final class ChannelExtensions {

    private ChannelExtensions() { }

    // send little-endian integer
    public static void writeInteger(AsynchronousFileChannel channel, int value) throws InterruptedException, ExecutionException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        writeInteger(buffer, value);
        buffer.flip();
        while (buffer.hasRemaining()) channel.write(buffer, 0).get(); // FIXME ?!
    }

    private static void writeInteger(ByteBuffer buffer, int value) {
        buffer.clear();
        buffer.put((byte)(value & 255)); value = value >> 8;
        buffer.put((byte)(value & 255)); value = value >> 8;
        buffer.put((byte)(value & 255)); value = value >> 8;
        buffer.put((byte)(value & 255));
    }

    // send Data as length + bytes
    public static void writeData(AsynchronousFileChannel channel, Data data) throws InterruptedException, ExecutionException {
        byte[] bytes = data.toByteArray();
        ChannelExtensions.writeInteger(channel, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) channel.write(buffer, 0).get(); // FIXME async?
    }
}
