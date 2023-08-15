package com.hazelcast.usercode;

import java.util.concurrent.CompletableFuture;

// represents a UserCode transport
public interface UserCodeTransport {

    CompletableFuture<Void> open();

    CompletableFuture<UserCodeMessage> invoke(UserCodeMessage message);

    void destroy();
}
