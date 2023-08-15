package com.hazelcast.usercode.transports;

import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransport;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class MultiplexTransportBase implements UserCodeTransport {

    // FIXME extract constants
    private final int timeout = 10;
    private final TimeUnit timeoutUnit = TimeUnit.SECONDS;

    private final Map<Long, CompletableFuture<UserCodeMessage>> futures = new HashMap<>();
    private final UserCodeMessage timeoutMessage = new UserCodeMessage(0, ".TIMEOUT", null);

    protected CompletableFuture<UserCodeMessage> createFuture(UserCodeMessage message) {

        CompletableFuture<UserCodeMessage> future = new CompletableFuture<UserCodeMessage>().completeOnTimeout(timeoutMessage, timeout, timeoutUnit);
        futures.put(message.getId(), future);
        return future;
    }

    protected void completeFuture(UserCodeMessage message) {

        CompletableFuture<UserCodeMessage> future = futures.remove(message.getId());
        if (future != null) {
            future.complete(message);
        }
    }

    protected void failFuture(UserCodeMessage message, Throwable t) {

        CompletableFuture<UserCodeMessage> future = futures.remove(message.getId());
        if (future != null) {
            future.completeExceptionally(t);
        }
    }

    protected void failFutures(Throwable t) {

        for (Map.Entry<Long, CompletableFuture<UserCodeMessage>> entry : futures.entrySet()) {
            entry.getValue().completeExceptionally(t);
        }
    }
}
