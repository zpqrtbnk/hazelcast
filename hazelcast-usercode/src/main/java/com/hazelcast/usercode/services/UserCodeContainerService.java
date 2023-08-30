/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.usercode.services;

import com.hazelcast.jet.jobbuilder.JobBuilderInfoMap;
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

    public UserCodeContainerService(String localMember, String address, int port, LoggingService logging) {
        super(localMember, logging);
        this.controller = new UserCodeRuntimeController(address, port, logging.getLogger(UserCodeRuntimeController.class));
        this.logger = logging.getLogger(UserCodeContainerService.class);
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException {

        JobBuilderInfoMap containerInfo = startInfo.childAsMap("service").childAsMap("container");

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

					// FIXME cleanup this, conditions are flaky
                    if (startInfo.childIsMap("transport")) {
                        JobBuilderInfoMap transportInfo = startInfo.childAsMap("transport");
						JobBuilderInfoMap grpcInfo = transportInfo.childAsMap("grpc", false);
						if (grpcInfo == null) {
							throw new UserCodeException("Invalid transport, must be 'grpc'.");
						}
						// FIXME overwriting what's in yaml?! why?! should test hasChild first!
                        grpcInfo.setChild("address", containerAddress);
                        grpcInfo.setChild("port", 5252); // FIXME or whatever the default gRPC port is
                    }
                    else {
                        if (startInfo.childIsString("transport") && !startInfo.childAsString("transport").equals("grpc")) {
                            throw new UserCodeException("Invalid transport, must be 'grpc'.");
                        }
						JobBuilderInfoMap grpcInfo = new JobBuilderInfoMap();
                        grpcInfo.setChild("address", containerAddress);
                        grpcInfo.setChild("port", 5252); // FIXME or whatever the default gRPC port is
                        JobBuilderInfoMap transportInfo = new JobBuilderInfoMap();
						transportInfo.setChild("grpc", grpcInfo);
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

