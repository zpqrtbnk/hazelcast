package com.hazelcast.usercode;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

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
            if (m.isException()) {
                byte[] exceptionMessageBytes = m.getPayload();
                String exceptionMessage = "Runtime exception." + (exceptionMessageBytes == null ? "" : " " + new String(exceptionMessageBytes, StandardCharsets.UTF_8));
                throw new UserCodeException(exceptionMessage);
            } else {
                return m.getPayload();
            }
        });
    }
}
