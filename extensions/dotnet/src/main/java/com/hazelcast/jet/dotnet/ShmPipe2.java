package com.hazelcast.jet.dotnet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ShmPipe2 implements IJetPipe {
    private final int SizeOfInt = Integer.BYTES;
    private final ShmPipe2Monitor monitor;

    private final int outWpOffset;
    private final int outRpOffset;
    private final int inWpOffset;
    private final int inRpOffset;

    private final int outDataOffset;
    private final int inDataOffset;

    private final String uid;
    private final Path filename;
    private final int capacity;
    private final int spinDelay;

    private final MappedByteBuffer mapBuffer;

    private final byte[] ibuffer = new byte[4];

    private int inrp, outwp;

    public ShmPipe2(boolean isOwner) throws IOException, IllegalArgumentException {
        this(isOwner, null, null, 1024, 500);
    }

    public ShmPipe2(boolean isOwner, String filename, String uid, int dataCapacity, int spinDelayMilliseconds) throws IOException, IllegalArgumentException {

        if (isOwner) throw new IllegalArgumentException("Owner mode not supported.");

        this.capacity = dataCapacity;
        this.spinDelay = spinDelayMilliseconds;
        this.monitor = new ShmPipe2Monitor(this);

        // total size
        int size = 4 * SizeOfInt + 2 * dataCapacity;

        // get the file
        this.uid = uid != null ? uid : UUID.randomUUID().toString();
        this.filename = filename != null
                ? Paths.get(filename)
                : Paths.get(System.getProperty("java.io.tmpdir"), "hz-shmpipe-" + this.uid);
        if (isOwner && Files.exists(this.filename)) throw new IllegalArgumentException("File " + this.filename + " already exists.");
        if (!isOwner && !Files.exists(this.filename)) {//throw new IllegalArgumentException("File " + this.filename + " does not exist.");
            // FIXME maybe we need to retry a bit?
            int count = 10;
            while (!Files.exists(this.filename) && --count > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                }
                catch (InterruptedException ie) {
                    // ??
                }
            }
            if (count == 0) throw new IllegalArgumentException("File " + this.filename + " does not exist.");
        }

        // open the mapped memory
        FileChannel channel = FileChannel.open(this.filename, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        mapBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // configure pointers
        if (isOwner)
        {
            outWpOffset = 0;
            outRpOffset = outWpOffset + SizeOfInt;
            inWpOffset = outRpOffset + SizeOfInt;
            inRpOffset = inWpOffset + SizeOfInt;
            outDataOffset = inRpOffset + SizeOfInt;
            inDataOffset = outDataOffset + capacity;
        }
        else
        {
            inWpOffset = 0;
            inRpOffset = inWpOffset + SizeOfInt;
            outWpOffset = inRpOffset + SizeOfInt;
            outRpOffset = outWpOffset + SizeOfInt;
            inDataOffset = outRpOffset + SizeOfInt;
            outDataOffset = inDataOffset + capacity;
        }
    }

    public String getUid() { return uid; }
    public Path getFilename() {
        return filename;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getSpinDelay() { return spinDelay; }

    public void destroy() {
        // TODO: we probably MUST destroy the map buffer, close the file, etc.
    }

    private Void writeImmediately(JetMessage message) {

        // read the read pointer
        int outrp = getInt(outRpOffset);

        // write
        writeInteger(message.getOperationId(), outrp);
        if (message.getOperationId() >= 0) {
            writeInteger(message.getBuffers().length, outrp);
            for (int i = 0; i < message.getBuffers().length; i++) write(message.getBuffers()[i], outrp);
        }

        // write the write pointer
        setInt(outWpOffset, outwp);

        return null;
    }

    public CompletableFuture<Void> write(JetMessage message) {

        int requiredBytes = message.getRequiredBytes();

        if (canWrite(requiredBytes)) {
            writeImmediately(message);
            return CompletableFuture.completedFuture(null);
        }

        return monitor
                .waitCanWrite(requiredBytes)
                .thenApply(x -> writeImmediately(message));
    }

    private JetMessage readImmediately() {

        // read the write pointer
        int inwp = getInt(inWpOffset);

        // read
        int id = readInteger(inwp);
        JetMessage message;
        if (id >= 0)
        {
            int count = readInteger(inwp);
            byte[][] buffers = new byte[count][];
            for (int i = 0; i < count; i++) buffers[i] = read(inwp);
            message = new JetMessage(id, buffers);
        }
        else
        {
            message = JetMessage.KissOfDeath;
        }

        // write the read pointer
        setInt(inRpOffset, inrp);

        return message;
    }

    public CompletableFuture<JetMessage> read() {

        if (canRead()) return CompletableFuture.completedFuture(readImmediately());

        return monitor
                .waitCanRead()
                .thenApply(x -> readImmediately());
    }

    public boolean canWrite(int requiredBytes)
    {
        // read the read pointer
        int outrp = mapBuffer.getInt(outRpOffset);

        // calc free space
        int freeBytes = outwp >= outrp
                ? capacity - outwp + outrp
                : outrp - outwp;

        // enough space?
        return freeBytes >= requiredBytes + 1; // 1-byte gap
    }

    private void write(byte[] buffer, int outrp)
    {
        writeInteger(buffer.length, outrp);
        if (buffer.length > 0) writeBytes(buffer, outrp);
    }

    private void writeInteger(int value, int outrp)
    {
        for (int i = 0; i < 4; i++) { ibuffer[i] = (byte) (value & 255); value >>= 8; }
        writeBytes(ibuffer, outrp);
    }

    private void writeBytes(byte[] buffer, int outrp)
    {
        int destination = outDataOffset + outwp;
        int limit = outwp >= outrp ? capacity : outrp;
        int length = Math.min(buffer.length, limit - outwp);
        outwp += length;
        if (outwp == capacity) outwp = 0;

        // https://stackoverflow.com/questions/15409727/missing-some-absolute-methods-on-bytebuffer
        // TODO: there may be a more efficient way to do this
        ((ByteBuffer)mapBuffer.duplicate().position(destination)).put(buffer, 0, length);

        if (length == buffer.length) return;

        destination = outDataOffset + outwp;
        int length2 = buffer.length - length;
        outwp += length2;

        ((ByteBuffer)mapBuffer.duplicate().position(destination)).put(buffer, length, length2);
    }

    public boolean canRead()
    {
        // read the write pointer
        int inwp = mapBuffer.getInt(inWpOffset);
        return inwp != inrp;
    }

    private byte[] read(int inwp)
    {
        int bufferLength = readInteger(inwp);
        byte[] buffer = new byte[bufferLength];
        if (bufferLength > 0) readBytes(buffer, inwp);
        return buffer;
    }

    private int readInteger(int inwp)
    {
        readBytes(ibuffer, inwp);
        int value = 0;
        for (int i = 0; i < 4; i++) value += ibuffer[i] << (i * 8);
        return value;
    }

    private void readBytes(byte[] buffer, int inwp)
    {
        int source = inDataOffset + inrp;
        int limit = inwp >= inrp ? inwp : capacity;
        int length = Math.min(buffer.length, limit - inrp);
        inrp += length;
        if (inrp == capacity) inrp = 0;

        ((ByteBuffer)mapBuffer.duplicate().position(source)).get(buffer, 0, length);

        if (length == buffer.length) return;

        source = inDataOffset + inrp;
        int length2 = buffer.length - length;
        inrp += length2;

        ((ByteBuffer)mapBuffer.duplicate().position(source)).get(buffer, length, length2);
    }

    private int getInt(int offset) {
        return mapBuffer.getInt(offset);
    }

    private void setInt(int offset, int value) {
        mapBuffer.putInt(offset, value);
    }
}
