package com.hazelcast.usercode.services;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class UserCodeRuntimeController {

    public CompletableFuture<String> createContainer(String image) {
        // TODO: implement
        return CompletableFuture.completedFuture("meh");
    }

    public CompletableFuture<Void> deleteContainer(String name) {
        // TODO: implement
        return null;
    }
}
