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

package com.hazelcast.usercode;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.jobbuilder.JobBuilderInfoMap;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.transports.grpc.GrpcTransport;
import com.hazelcast.usercode.transports.sharedmemory.SharedMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class UserCodeServiceBase implements UserCodeService, SerializationServiceAware {

    private final String localMember;
    protected final LoggingService logging;
    protected SerializationService serializationService;

    public UserCodeServiceBase(String localMember, LoggingService logging) {

        this.localMember = localMember;
        this.logging = logging;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    protected UserCodeTransport createTransport(UserCodeRuntimeInfo runtimeInfo) {

        // create the transport
        String transportName;
        JobBuilderInfoMap transportInfo;
        if (runtimeInfo.childIsString("transport")) {
            transportName = runtimeInfo.childAsString("transport");
            transportInfo = new JobBuilderInfoMap();
        }
        else {
            JobBuilderInfoMap info = runtimeInfo.childAsMap("transport");
            transportName = info.uniqueChildName();
            transportInfo = info.childAsMap(transportName);
        }

        UUID uniqueId = runtimeInfo.childAsUUID("uid");
        transportInfo.setChild("uid", uniqueId);

        UserCodeTransport transport;
        switch (transportName) {
            case "grpc":
                transport = new GrpcTransport(transportInfo, logging);
                break;
            case "shared-memory":
                transport = new SharedMemoryTransport(transportInfo, logging);
                break;
            default:
                throw new UserCodeException("Unsupported transport mode '" + transportName + "'.");
        }
        return transport;
    }

    protected CompletableFuture<UserCodeRuntime> initialize(UserCodeRuntimeBase runtime) {

        runtime.getTransport().open(); // FIXME could this be async?

        byte[] connectArgs = localMember.getBytes(StandardCharsets.US_ASCII);

        return runtime.getTransport()
                .invoke(new UserCodeMessage(0, UserCodeConstants.FunctionNames.Connect, connectArgs))
                .thenApply(response -> {
                    if (response.isError()) {
                        throw new UserCodeException("Exception in CONNECT: " + response.getErrorMessage());
                    }
                    return runtime;
                });
    }

    protected CompletableFuture<Void> terminate(UserCodeRuntimeBase runtime) {

        return runtime.getTransport()
                .invoke(new UserCodeMessage(0, UserCodeConstants.FunctionNames.End, new byte[0]))
                .thenApply(response -> {
                    if (response.isError()) {
                        throw new UserCodeException("Exception in END: " + response.getErrorMessage());
                    }
                    return null;
                });
    }

    public Future<Void> destroyRuntime(UserCodeRuntime runtime) {
        return CompletableFuture.completedFuture(null);
    }
}
