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

public abstract class UserCodeRuntimeBase implements UserCodeRuntime, UserCodeTransportReceiver {

    private final UserCodeService userCodeService;
    private final Map<Long, CompletableFuture<byte[]>> futures = new HashMap<>();
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
    public CompletableFuture<Data> invoke(String functionName, Data payload) {
        return invoke(functionName, payload.toByteArray()).thenApply(HeapData::new);
    }

    @Override
    public CompletableFuture<byte[]> invoke(String functionName, byte[] payload) {

        // TODO:
        //  - what about timeouts? can we leak?

        // using gRPC... each call is one unique ? what's 'streaming' RPC?
        // and using shared memory, how should it work?
        // one loop monitoring the shmem, and assigning tasks to threads?
        // and, client-side, one loop monitoring the shmem and completing invocations

        long id = ids.getAndIncrement();
        UserCodeMessage message = new UserCodeMessage(id, functionName, payload);
        transport.send(message);
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        futures.put(id, future);
        return future;
    }

    @Override
    public void receive(UserCodeMessage message) {

        CompletableFuture<byte[]> future = futures.get(message.getId());
        futures.remove(message.getId());

        if (message.isException()) {
            byte[] payload = message.getPayload();
            String exceptionMessage = "Runtime exception." + (payload == null ? "" : " " + new String(payload, StandardCharsets.UTF_8));
            future.completeExceptionally(new UserCodeException(exceptionMessage));
        } else {
            future.complete(message.getPayload());
        }
    }
}
