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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

// provides a base class for UserCodeRuntime implementations
public abstract class UserCodeRuntimeBase implements UserCodeRuntime {

    private final UserCodeService userCodeService;
    private final AtomicLong ids = new AtomicLong();
    private final UserCodeTransport transport;
    private final SerializationService serializationService;

    public UserCodeRuntimeBase(UserCodeService userCodeService, UserCodeTransport transport, SerializationService serializationService) {

        this.userCodeService = userCodeService;
        this.transport = transport;
        this.serializationService = serializationService;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public UserCodeService getUserCodeService() {
        return userCodeService;
    }

    public UserCodeTransport getTransport() {
        return transport;
    }

    @Override
    public CompletableFuture<?> invoke(String functionName, Object input) {
        byte[] payload = input == null ? new byte[0] : serializationService.toData(input).toByteArray();
        return invoke(functionName, payload).thenApply(x -> serializationService.toObject(new HeapData(x)));
    }

    @Override
    public CompletableFuture<Data> invoke(String functionName, Data payload) {
        return invoke(functionName, payload.toByteArray()).thenApply(HeapData::new);
    }

    @Override
    public CompletableFuture<byte[]> invoke(String functionName, byte[] payload) {

        long id = ids.getAndIncrement();
        UserCodeMessage message = new UserCodeMessage(id, functionName, payload);

        return transport.invoke(message).thenApply(m -> {
            if (m.isError()) {
                throw new UserCodeException("Runtime exception: " + m.getErrorMessage());
            } else {
                return m.getPayload();
            }
        });
    }
}
