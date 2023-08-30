/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public boolean isError() {
        return UserCodeConstants.FunctionNames.Error.equals(functionName);
    }

    public String getErrorMessage() {
        return payload == null ? "" : " " + new String(payload, StandardCharsets.UTF_8);
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
