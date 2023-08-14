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
import com.hazelcast.jet.usercoderuntime.IUserCodeRuntime;
import com.hazelcast.jet.usercoderuntime.IUserCodeRuntimeService;
import com.hazelcast.jet.usercoderuntime.RuntimeConfig;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntimeContext;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntimeServiceConfig;
import com.hazelcast.logging.ILogger;
import io.grpc.ManagedChannel;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class UserCodeServiceImpl implements IUserCodeRuntimeService {

    private static final int KEEP_ALIVE_TIME_SECONDS = 300;
    private static final int KEEP_ALIVE_TIMEOUT_SECONDS = 10;
    private ProcessorSupplier.Context context;
    private ILogger logger;
    // Channel to runtime controller.
    private ManagedChannel ctrlChannel;
    private JetToRuntimeControllerGrpc.JetToRuntimeControllerFutureStub stub;
    private JetToRuntimeControllerGrpc.JetToRuntimeControllerBlockingStub stubBlocking;


    public UserCodeServiceImpl(ProcessorSupplier.Context context, UserCodeRuntimeServiceConfig config) {

        this.logger = context.hazelcastInstance().getLoggingService()
                .getLogger(getClass().getPackage().getName());

        this.ctrlChannel = config.channelFn().apply(config.getControllerAddress(), config.getControllerPort())
                .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                // Configure retry for idempotent
                .enableRetry()
                .usePlaintext()
                .build();

        stub = JetToRuntimeControllerGrpc.newFutureStub(ctrlChannel);

        stubBlocking = JetToRuntimeControllerGrpc.newBlockingStub(ctrlChannel);
    }


    @Override
    public IUserCodeRuntime startRuntime(String name, RuntimeConfig config) {

        RuntimeRequest request = RuntimeRequest.newBuilder()
                .setName(name)
                .setImageName(config.getImageName())
                .setListeningPort(config.getListeningPort())
                .build();

        RuntimeResponse result = stubBlocking.create(request);
        UserCodeRuntimeGrpcImpl runtime = new UserCodeRuntimeGrpcImpl(new UserCodeRuntimeContext(context, result));

        return runtime;
    }

    @Override
    public Future destroyRuntimeAsync(String uuid) {

        RuntimeDestroyRequest request = RuntimeDestroyRequest
                .newBuilder()
                .setUuid(uuid)
                .build();

        return stub.destroy(request);
    }

}
