package com.hazelcast.usercode;

import java.nio.charset.StandardCharsets;

public final class UserCodeMessage {

    private final static int SizeOfInt = Integer.BYTES;

    private final long id;
    private final String functionName;
    private final byte[] payload;

    public UserCodeMessage(long id, String functionName, byte[] payload) {

        this.id = id;
        this.functionName = functionName;
        this.payload = payload;
    }

    public long getId() {
        return id;
    }

    public String getFunctionName() {
        return functionName;
    }

    public byte[] getPayload() {
        return payload;
    }

    // determines whether a message is an exception message
    public boolean isException() {
        return functionName.equals(".EXCEPTION"); // TODO: name for special functions? prefix with dot?
    }

    // determines the number of bytes required to carry the message
    public int getRequiredBytes()
    {
        int requiredBytes = 4 * SizeOfInt; // id (long) + functionName length (int) + payload length (int)
        requiredBytes += functionName.getBytes(StandardCharsets.US_ASCII).length; // + functionName bytes
        if (payload != null) requiredBytes += payload.length; // + payload bytes
        return requiredBytes;
    }
}
