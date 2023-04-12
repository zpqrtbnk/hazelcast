package com.hazelcast.jet.dotnet;

// represents a jet message
public class JetMessage {

    private final int SizeOfInt = Integer.BYTES;
    private final int operationId;
    private final byte[][] buffers;

    // initializes a new jet message
    public JetMessage(int operationId, byte[][] buffers) {

        this.operationId = operationId;
        this.buffers = buffers;
    }

    // gets the unique kiss-of-death message
    public static JetMessage KissOfDeath = new JetMessage(-1, null);

    // gets the unique identifier of the operation
    public int getOperationId() {

        return operationId;
    }

    // gets the buffers of the operation
    public byte[][] getBuffers() {

        return buffers;
    }

    // determines whether a message is the kiss-of-death message
    public boolean isKissOfDeath() {

        return operationId == -1;
    };

    // determines the number of bytes required to carry the message
    public int getRequiredBytes()
    {

        int requiredBytes = SizeOfInt + SizeOfInt; // id + buffers.length
        if (buffers != null) {
            for (int i = 0; i < buffers.length; i++) requiredBytes += buffers[i].length + SizeOfInt;
        }
        return requiredBytes;
    }
}
