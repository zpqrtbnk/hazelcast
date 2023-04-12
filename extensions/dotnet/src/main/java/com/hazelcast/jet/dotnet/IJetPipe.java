package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

// defines a jet pipe
public interface IJetPipe {

    // writes a jet message to the pipe
    // blocks until the pipe has enough space
    // FIXME: how can we interrupt it?
    CompletableFuture<Void> write(JetMessage message);

    // reads a jet message from the pipe
    // blocks until the pipe has a message
    // FIXME: how can we interrupt it?
    CompletableFuture<JetMessage> read();

    // destroy the pipe
    void destroy();
}
