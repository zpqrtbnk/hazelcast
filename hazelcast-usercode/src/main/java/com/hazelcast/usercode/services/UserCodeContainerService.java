package com.hazelcast.usercode.services;

import com.hazelcast.jet.jobbuilder.InfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeContainerRuntime;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// a UserCodeService that talks to the UserCodeController API and executes each runtime in a separate container
public final class UserCodeContainerService extends UserCodeServiceBase {

    // TODO: implement this, Emre is currently working on it
    // TODO: implement logging when container starts and stops etc

    private final ILogger logger;
    private final UserCodeRuntimeController controller;

    public UserCodeContainerService(UserCodeRuntimeController controller, LoggingService logging) {
        super(logging);
        this.controller = controller;
        this.logger = logging.getLogger(UserCodeContainerService.class);
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException {

        InfoMap containerInfo = startInfo.childAsMap("container");

        // allocate the runtime unique identifier
        UUID uniqueId = UUID.randomUUID();
        startInfo.setChild("uid", uniqueId);

        // assuming we have a controller (initialized with address:port or anything, how?)
        // we start the container from an image and receive back its address which uniquely identifies it
        // and, for now, assume that the container has a gRPC service listening on a pre-agreed port
        // should we want to do shared memory... we'd have to pass more infos to createContainer
        String image = containerInfo.childAsString("image");

        return controller
                .createContainer(image)
                .thenApply(containerAddress -> {

                    if (startInfo.childIsMap("transport")) {
                        InfoMap transportInfo = startInfo.childAsMap("transport");
                        transportInfo.setChild("address", containerAddress);
                        transportInfo.setChild("port", 5252); // FIXME or whatever the default gRPC port is
                    }
                    else {
                        if (startInfo.childIsString("transport") && !startInfo.childAsString("transport").equals("grpc")) {
                            throw new UserCodeException("Invalid transport, must be 'grpc'.");
                        }
                        InfoMap transportInfo = new InfoMap();
                        transportInfo.setChild("address", containerAddress);
                        transportInfo.setChild("port", 5252); // FIXME or whatever the default gRPC port is
                        startInfo.setChild("transport", transportInfo);
                    }
                    UserCodeTransport transport = createTransport(startInfo);

                    return new UserCodeContainerRuntime(this, transport, serializationService, containerAddress);
                })
                .thenCompose(this::initialize);
    }

    @Override
    public CompletableFuture<Void> destroyRuntime(UserCodeRuntime runtime) {

        if (!(runtime instanceof UserCodeContainerRuntime)) {
            throw new UnsupportedOperationException("runtime is not UserCodeContainerRuntime");
        }
        UserCodeContainerRuntime containerRuntime = (UserCodeContainerRuntime) runtime;
        String containerAddress = containerRuntime.getContainerName();
        return terminate(containerRuntime)
                .thenCompose(x -> controller.deleteContainer(containerAddress));
    }
}

