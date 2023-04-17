package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

// defines a jet pipe
public interface IJetPipe {

    // FIXME: what would be the equivalent of .NET cancellation for the futures?

    // writes a jet message to the pipe
    // blocks until the pipe has enough space
    CompletableFuture<Void> write(JetMessage message);

    // reads a jet message from the pipe
    // blocks until the pipe has a message
    CompletableFuture<JetMessage> read();

    // destroy the pipe
    void destroy();
}
