/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */

package com.hazelcast.jet.usercoderuntime.impl;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.usercoderuntime.FailedRuntimeRequestException;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntime;
import com.hazelcast.jet.usercoderuntime.RuntimeConfig;
import com.hazelcast.jet.usercoderuntime.RuntimeServiceConfig;
import com.hazelcast.jet.usercoderuntime.impl.controller.ControllerGrpc;
import com.hazelcast.jet.usercoderuntime.impl.controller.CreateRequest;
import com.hazelcast.jet.usercoderuntime.impl.controller.CreateResponse;
import com.hazelcast.jet.usercoderuntime.impl.controller.DeleteRequest;
import com.hazelcast.spi.properties.ClusterProperty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class UserCodeRuntimeService {
    private static final int DEADLINE_FOR_INVOCATION = 10;
    private static UserCodeRuntimeService service;
    // Channel to runtime controller.
    private ManagedChannel ctrlChannel;
    private ControllerGrpc.ControllerFutureStub stub;
    private ControllerGrpc.ControllerBlockingStub stubBlocking;

    private UserCodeRuntimeService(RuntimeServiceConfig config) {
        this.ctrlChannel = ManagedChannelBuilder
                .forAddress(config.getControllerAddress(), config.getControllerGrpcPort())
                .keepAliveTime(config
                                .getProperties()
                                .getInteger(ClusterProperty.USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_SECONDS),
                        TimeUnit.SECONDS)
                .keepAliveTimeout(config
                                .getProperties()
                                .getInteger(ClusterProperty.USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_TIMEOUT_SECONDS),
                        TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .enableRetry()
                .usePlaintext()
                .build();

        stub = ControllerGrpc.newFutureStub(ctrlChannel);
        stubBlocking = ControllerGrpc.newBlockingStub(ctrlChannel);
    }

    /**
     * Gets the service instance.
     *
     * @since 5.4
     */
    public static synchronized UserCodeRuntimeService getInstance() {
        if (service == null) {
            service = new UserCodeRuntimeService(new RuntimeServiceConfig());
        }

        return service;
    }

    /**
     * Gets the service object. If the service is not created before, it will apply the given configuration
     * while creating the service.
     * @param config
     *
     * @since 5.4
     */
    public static synchronized UserCodeRuntimeService getInstance(RuntimeServiceConfig config) {

        if (service == null) {
            service = new UserCodeRuntimeService(config);
        }

        return service;
    }

    /**
     * Creates a {@link UserCodeRuntime} instance by using the user code runtime controller within given duration
     * otherwise throws.
     *
     * @since 5.4
     */
    public UserCodeRuntime startRuntime(ProcessorSupplier.Context context, RuntimeConfig config, int timeOutSeconds) {

        CreateRequest request = CreateRequest.newBuilder()
                .setImage(config.getImageName())
                .build();

        try {

            CreateResponse result;

            if (timeOutSeconds > 0) {
                result = stubBlocking
                        .withDeadlineAfter(timeOutSeconds, TimeUnit.SECONDS)
                        .create(request);
            } else {
                result = stubBlocking.create(request);
            }

            // Add check when other type of transport introduced-> GRPC, Shared Memory
            UserCodeRuntimeGrpcImpl runtime = new UserCodeRuntimeGrpcImpl(context, result, config);
            return runtime;
        } catch (Exception e) {
            context.hazelcastInstance()
                    .getLoggingService()
                    .getLogger("UserCodeRuntimeService")
                    .severe("Request for starting a new user code runtime failed.", e);
            throw new FailedRuntimeRequestException("Request for starting a new user code runtime failed.", e);
        }
    }

    /**
     * Creates a {{@link UserCodeRuntime}} instance by using the user code runtime controller.
     *
     * @since 5.4
     */
    public UserCodeRuntime startRuntime(ProcessorSupplier.Context context, RuntimeConfig config) {

        return startRuntime(context, config, -1);
    }

    /**
     * Destroys the {@link UserCodeRuntime} instance by name.
     * <p>
     * Note: It only destroys the remote counterpart of the {@link UserCodeRuntime} instance.
     * In order to release local resources of the instance, you should destroy it separately.
     * </p>
     * @since 5.4
     */
    public Future destroyRuntimeAsync(String name) {

        // If we observe the output and act according to result then what thread will be responsible to manage or retry
        // that request since it is an async call.

        DeleteRequest request = DeleteRequest
                .newBuilder()
                .setName(name)
                .build();

        return stub.withDeadlineAfter(DEADLINE_FOR_INVOCATION, TimeUnit.SECONDS).withWaitForReady().delete(request);
    }
}
