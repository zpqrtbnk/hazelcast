package com.hazelcast.jet.dotnet;

public class JetMessage {

    private final int SizeOfInt = Integer.BYTES;
    private final int operationId;
    private final byte[][] buffers;

    public JetMessage(int operationId, byte[][] buffers) {
        this.operationId = operationId;
        this.buffers = buffers;
    }

    public static JetMessage KissOfDeath = new JetMessage(-1, null);

    public int getOperationId() {
        return operationId;
    }

    public byte[][] getBuffers() {
        return buffers;
    }

    public boolean isKissOfDeath() {
        return operationId == -1;
    };

    public int getRequiredBytes()
    {
        int requiredBytes = SizeOfInt + SizeOfInt; // id + buffers.length
        if (buffers != null) {
            for (int i = 0; i < buffers.length; i++) requiredBytes += buffers[i].length + SizeOfInt;
        }
        return requiredBytes;
    }
}
