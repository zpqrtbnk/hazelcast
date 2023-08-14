package com.hazelcast.usercode.transports.sharedmemory;

import com.hazelcast.usercode.UserCodeException;
import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransportReceiver;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public final class SharedMemoryPipe {

    private final static int SizeOfInt = Integer.BYTES;
    private final UserCodeTransportReceiver receiver;
    private final Queue<UserCodeMessage> writeQueue = new ConcurrentLinkedQueue<>();

    private final int outWpOffset;
    private final int outRpOffset;
    private final int inWpOffset;
    private final int inRpOffset;

    private final int outDataOffset;
    private final int inDataOffset;

    private final UUID uniqueId;
    private final Path filepath;
    private final int dataCapacityBytes;
    private final int spinDelayMilliseconds;
    private final int openTimeoutMilliseconds;

    private FileChannel channel;
    private MappedByteBuffer mapBuffer;
    private SharedMemoryPipeThread pipeThread;

    private final byte[] ibuffer = new byte[4];
    private final byte[] lbuffer = new byte[8];

    private int inrp, outwp;

    public SharedMemoryPipe(UUID uniqueId, Path filepath, UserCodeTransportReceiver receiver, int dataCapacityBytes, int spinDelayMilliseconds, int openTimeoutMilliseconds) {

        this.uniqueId = uniqueId;
        this.filepath = filepath;
        this.receiver = receiver;
        this.dataCapacityBytes = dataCapacityBytes;
        this.spinDelayMilliseconds = spinDelayMilliseconds;
        this.openTimeoutMilliseconds = openTimeoutMilliseconds;

        // configure pointers
        inWpOffset = 0;
        inRpOffset = inWpOffset + SizeOfInt;
        outWpOffset = inRpOffset + SizeOfInt;
        outRpOffset = outWpOffset + SizeOfInt;
        inDataOffset = outRpOffset + SizeOfInt;
        outDataOffset = inDataOffset + this.dataCapacityBytes;

        // closed
        inrp = outwp = -1;
    }

    public UUID getUniqueId() {
        return uniqueId;
    }

    public void open() {

        // get the file
        int elapsedMilliseconds = 0;
        final int delayMilliseconds = 500;
        while (!Files.exists(filepath) && elapsedMilliseconds < openTimeoutMilliseconds) {
            try {
                TimeUnit.MILLISECONDS.sleep(delayMilliseconds);
                elapsedMilliseconds += delayMilliseconds;
            }
            catch (InterruptedException ie) {
                elapsedMilliseconds = Integer.MAX_VALUE;
            }
        }

        if (elapsedMilliseconds >= openTimeoutMilliseconds) {
            throw new UserCodeException("Failed to open shared memory at '" + filepath + "'.");
        }

        // total size of the shared memory area
        // = 4 pointers + capacity for read + write
        int size = 4 * SizeOfInt + 2 * dataCapacityBytes;

        // open the mapped memory area
        try {
            channel = FileChannel.open(filepath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            mapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        }
        catch (IOException ex) {
            throw new UserCodeException("Failed to open shared memory at '" + filepath + "'.", ex);
        }

        mapBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // open
        inrp = outwp = 0;

        // start the monitoring thread
        pipeThread = new SharedMemoryPipeThread(this, spinDelayMilliseconds);
        pipeThread.start();
    }

    // writes a message i.e. queue it for writing
    public void write(UserCodeMessage message) {
        if (outwp < 0) {
            throw new IllegalStateException("Pipe is closed.");
        }
        writeQueue.add(message);
    }

    // tries to write a queued message to shared memory
    // invoked by the thread
    public boolean send() {

        UserCodeMessage message = writeQueue.peek();

        if (message == null) {
            // no message in queue
            return false;
        }

        int requiredBytes = message.getRequiredBytes();

        if (requiredBytes + 1 > dataCapacityBytes) {
            // will never be able to fit this message in the buffer
            throw new IllegalArgumentException("Value " + requiredBytes + " exceeds capacity " + dataCapacityBytes + ".");
        }

        if (!canWrite(message.getRequiredBytes())) {
            // cannot write now, maybe later
            return false;
        }

        // remove and write
        writeQueue.remove();
        writeSharedMemory(message);
        return true;
    }

    // tries to read a message from shared memory
    // invoked by the thread
    public boolean receive() {

        if (canRead()) {
            UserCodeMessage message = readSharedMemory();
            receiver.receive(message);
            return true;
        }
        else {
            return false;
        }
    }

    // invoked by the thread
    public void fail(Exception ex) {
        inrp = outwp = -1;
        // FIXME we should have a logger? or bubble it somehow?
        System.err.println(ex);
        destroy();
    }

    public void destroy() {

        if (inrp < 0 && outwp < 0) return;

        pipeThread.stopAndJoin();
        inrp = outwp = -1;
        try {
            channel.close();
        }
        catch (Exception ex) {
            // ignore
        }
    }

    private void writeSharedMemory(UserCodeMessage message) {

        // read the read pointer
        int outrp = getInt(outRpOffset);

        // write
        writeLong(message.getId(), outrp);
        writeString(message.getFunctionName(), outrp);
        byte[] payload = message.getPayload();
        if (payload == null) {
            writeInteger(0, outrp);
        }
        else {
            write(payload, outrp);
        }

        // write the write pointer
        setInt(outWpOffset, outwp);
    }

    public UserCodeMessage readSharedMemory() {

        // read the write pointer
        int inwp = getInt(inWpOffset);

        // read
        long id = readLong(inwp);
        String functionName = readString(inwp);
        byte[] payload = read(inwp);
        UserCodeMessage message = new UserCodeMessage(id, functionName, payload);

        // write the read pointer
        setInt(inRpOffset, inrp);

        return message;
    }

    public boolean canWrite(int requiredBytes) {

        // read the read pointer
        int outrp = mapBuffer.getInt(outRpOffset);

        // calc free space
        int freeBytes = outwp >= outrp
                ? dataCapacityBytes - outwp + outrp
                : outrp - outwp;

        // enough space?
        return freeBytes >= requiredBytes + 1; // 1-byte gap
    }

    private void write(byte[] buffer, int outrp) {

        writeInteger(buffer.length, outrp);
        if (buffer.length > 0) writeBytes(buffer, outrp);
    }

    private void writeString(String value, int outrp) {

        byte[] buffer = value.getBytes(StandardCharsets.US_ASCII);
        write(buffer, outrp);
    }

    private void writeInteger(int value, int outrp) {

        for (int i = 0; i < 4; i++) { ibuffer[i] = (byte) (value & 255); value >>= 8; }
        writeBytes(ibuffer, outrp);
    }

    private void writeLong(long value, int outrp) {

        for (int i = 0; i < 8; i++) { lbuffer[i] = (byte) (value & 255); value >>= 8; }
        writeBytes(lbuffer, outrp);
    }

    private void writeBytes(byte[] buffer, int outrp) {

        int destination = outDataOffset + outwp;
        int limit = outwp >= outrp ? dataCapacityBytes : outrp;
        int length = Math.min(buffer.length, limit - outwp);
        outwp += length;
        if (outwp == dataCapacityBytes) outwp = 0;

        ByteBuffer writeBuffer = mapBuffer.duplicate(); // FIXME should allocate only once + same for reading

        writeBuffer.position(destination).put(buffer, 0, length);

        if (length == buffer.length) return;

        destination = outDataOffset + outwp;
        int length2 = buffer.length - length;
        outwp += length2;

        writeBuffer.position(destination).put(buffer, length, length2);
    }

    public boolean canRead() {

        // read the write pointer
        int inwp = mapBuffer.getInt(inWpOffset);
        return inwp != inrp;
    }

    private byte[] read(int inwp) {

        int bufferLength = readInteger(inwp);
        byte[] buffer = new byte[bufferLength];
        if (bufferLength > 0) readBytes(buffer, inwp);
        return buffer;
    }

    private String readString(int inwp) {

        byte[] buffer = read(inwp);
        return buffer.length == 0 ? "" : new String(buffer, StandardCharsets.US_ASCII);
    }

    private int readInteger(int inwp) {

        readBytes(ibuffer, inwp);
        int value = 0;
        for (int i = 0; i < 4; i++) value += Byte.toUnsignedInt(ibuffer[i]) << (i * 8);
        return value;
    }

    private long readLong(int inwp) {

        readBytes(lbuffer, inwp);
        long value = 0;
        for (int i = 0; i < 8; i++) value += Byte.toUnsignedLong(lbuffer[i]) << (i * 8);
        return value;
    }

    private void readBytes(byte[] buffer, int inwp) {

        int source = inDataOffset + inrp;
        int limit = inwp >= inrp ? inwp : dataCapacityBytes;
        int length = Math.min(buffer.length, limit - inrp);
        inrp += length;
        if (inrp == dataCapacityBytes) inrp = 0;

        ByteBuffer readBuffer = mapBuffer.duplicate(); // FIXME should allocate only once + same for writing

        readBuffer.position(source).get(buffer, 0, length);

        if (length == buffer.length) return;

        source = inDataOffset + inrp;
        int length2 = buffer.length - length;
        inrp += length2;

        readBuffer.position(source).get(buffer, length, length2);
    }

    private int getInt(int offset) {

        return mapBuffer.getInt(offset);
    }

    private void setInt(int offset, int value) {

        mapBuffer.putInt(offset, value);
    }
}
