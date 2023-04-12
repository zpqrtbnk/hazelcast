package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

public interface IJetPipe {

    CompletableFuture<Void> write(JetMessage message);

    CompletableFuture<JetMessage> read();
}
