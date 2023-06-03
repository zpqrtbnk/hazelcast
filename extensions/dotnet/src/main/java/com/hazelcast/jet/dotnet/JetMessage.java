package com.hazelcast.jet.dotnet;

import java.nio.charset.StandardCharsets;

// represents a jet message
public class JetMessage {

    private final static int SizeOfInt = Integer.BYTES;
    private final String transformName;
    private final int operationId;
    private final byte[][] buffers;

    // initializes a new jet message
    public JetMessage(String transformName, int operationId, byte[][] buffers) {

        this.transformName = transformName;
        this.operationId = operationId;
        this.buffers = buffers;
    }

    // gets the unique kiss-of-death message
    public static JetMessage KissOfDeath = new JetMessage("",-1, null);

    // gets the name of the transformation;
    public String getTransformName() {

        return transformName;
    }

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

    // determines whether a message is an exception message
    public boolean isException() {

        return transformName.length() == 0;
    }

    // determines the number of bytes required to carry the message
    public int getRequiredBytes()
    {

        int requiredBytes = 3 * SizeOfInt; // transform + id + buffers.length
        requiredBytes += transformName.getBytes(StandardCharsets.US_ASCII).length;
        if (buffers != null) {
            for (byte[] buffer : buffers) requiredBytes += buffer.length + SizeOfInt;
        }
        return requiredBytes;
    }
}
