package com.hazelcast.usercode.services;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeContainerRuntime;
import com.hazelcast.usercode.transports.grpc.GrpcTransport;

import java.util.concurrent.CompletableFuture;

// a UserCodeService that talks to the UserCodeController API and executes each runtime in a separate container
public final class UserCodeContainerService implements UserCodeService, SerializationServiceAware {

    // TODO: implement this, Emre is currently working on it
    // TODO: implement logging when container starts and stops etc

    private final LoggingService logging;
    private SerializationService serializationService;

    public UserCodeContainerService(LoggingService logging) {

        this.logging = logging;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeStartInfo startInfo) throws UserCodeException {

        String mode = startInfo.get("mode");
        if (!"container".equals(mode)) {
            throw new UserCodeException("Cannot start a mode '" + mode + "' runtime, expecting mode 'container'.");
        }

        String image = startInfo.get("image");
        String containerId = ""; // FIXME maybe we get it from startRuntime?
        String address = "";
        int port = 80;

        return startRuntime().thenApply(x -> {
            UserCodeTransport transport = new GrpcTransport(address, port, null); // FIXME args?
            UserCodeRuntime runtime = new UserCodeContainerRuntime(this, transport, serializationService, containerId);
            return runtime;
        });
    }

    private CompletableFuture<Void> startRuntime() {
        // TODO: use the rest API to start the container
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> destroyRuntime(UserCodeRuntime runtime) {

        if (!(runtime instanceof UserCodeContainerRuntime)) {
            throw new UnsupportedOperationException("runtime is not UserCodeContainerRuntime");
        }
        UserCodeContainerRuntime containerRuntime = (UserCodeContainerRuntime) runtime;
        return stopRuntime(containerRuntime.getContainerId());
    }

    private CompletableFuture<Void> stopRuntime(String uniqueName) {

        // TODO: use the rest API to destroy the container
        throw new UnsupportedOperationException();
    }
}

