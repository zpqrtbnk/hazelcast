package com.hazelcast.usercode;

import java.util.concurrent.CompletableFuture;

// represents a UserCode transport
public interface UserCodeTransport {

    void setReceiver(UserCodeTransportReceiver receiver);

    CompletableFuture<Void> open();

    CompletableFuture<Void> send(UserCodeMessage message);

    void destroy();
}
