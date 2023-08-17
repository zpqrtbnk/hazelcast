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

package com.hazelcast.jet.usercoderuntime.impl;

import com.google.protobuf.ByteString;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.grpc.impl.GrpcUtil;
import com.hazelcast.jet.usercoderuntime.RuntimeConfig;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntime;
import com.hazelcast.jet.usercoderuntime.RuntimeType;
import com.hazelcast.jet.usercoderuntime.RuntimeTransportType;
import com.hazelcast.jet.usercoderuntime.impl.controller.CreateResponse;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.JetToUserCodeRuntimeGrpc;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implements the {@link UserCodeRuntime} as grpc client. It establishes a GRPC connection between {@link UserCodeRuntime}
 * object and remote User Code Runtime Container, and manages the stream. You do not deal with this class directly.
 * {@link UserCodeRuntimeService} initializes it based on given config and response from User Code Runtime Controller
 * in the cluster.
 *
 * @since 5.4
 */
class UserCodeRuntimeGrpcImpl implements UserCodeRuntime {

    private String name;
    private String imageName;
    private int port;
    private ProcessorSupplier.Context context;
    private RuntimeTransportType communicationType;
    private RuntimeType runtimeType;
    private ManagedChannel channel;
    private ILogger logger;
    private StreamObserver<InputListOfMessage> streamStubAsBatch;
    private StreamObserver<InputMessage> streamStub;
    private Queue<CompletableFuture<List<Data>>> futureBatchQueue;
    private Queue<CompletableFuture<Data>> futureSingleQueue;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile Throwable exceptionInOutputObserver;

    /**
     * @param context
     */
    UserCodeRuntimeGrpcImpl(ProcessorSupplier.Context context,
                            CreateResponse response,
                            RuntimeConfig config) {
        this.context = context;
        this.logger = context.hazelcastInstance().getLoggingService()
                .getLogger(getClass().getPackage().getName());

        this.communicationType = RuntimeTransportType.GRPC;
        this.runtimeType = config.getRuntimeType();
        this.name = response.getName();
        this.port = config.getPort();
        this.imageName = config.getImageName();

        this.channel = ManagedChannelBuilder.forAddress(name, port)
                .keepAliveTime(config
                                .getProperties()
                                .getInteger(ClusterProperty.USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_SECONDS),
                        TimeUnit.SECONDS)
                .keepAliveTimeout(config
                                .getProperties()
                                .getInteger(ClusterProperty.USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_TIMEOUT_SECONDS),
                        TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                // TODO: consider retry for idempotent
                .enableRetry()
                .usePlaintext()
                .build();
    }

    @Override
    public CompletableFuture<Data> invoke(Data payload) {
        checkForServerError();

        if (streamStub == null) {
            futureSingleQueue = new ConcurrentLinkedQueue<>();
            JetToUserCodeRuntimeGrpc.JetToUserCodeRuntimeStub stub = JetToUserCodeRuntimeGrpc.newStub(channel);
            streamStub = stub.invoke(new OutputMessageObserver());
        }

        InputMessage message = InputMessage.newBuilder()
                .setPayLoad(ByteString.copyFrom(payload.toByteArray()))
                .build();

        CompletableFuture<Data> future = new CompletableFuture<>();
        futureSingleQueue.add(future);
        streamStub.onNext(message);

        return future;
    }

    @Override
    public CompletableFuture<List<Data>> invoke(List<Data> payloads) {
        checkForServerError();

        if (streamStubAsBatch == null) {
            futureBatchQueue = new ConcurrentLinkedQueue<>();
            JetToUserCodeRuntimeGrpc.JetToUserCodeRuntimeStub stub = JetToUserCodeRuntimeGrpc.newStub(channel);
            streamStubAsBatch = stub.invokeBatch(new OutputListOfMessageObserver());
        }

        //Prepare the batch and put to stream.
        InputListOfMessage.Builder messageBuilder = InputListOfMessage.newBuilder();

        for (Data payLoad : payloads) {
            messageBuilder.addPayLoad(ByteString.copyFrom(payLoad.toByteArray()));
        }

        CompletableFuture<List<Data>> future = new CompletableFuture<>();
        futureBatchQueue.add(future);
        streamStubAsBatch.onNext(messageBuilder.build());

        return future;
    }

    @Override
    public RuntimeTransportType getTransportType() {
        return communicationType;
    }

    @Override
    public RuntimeType getRuntimeType() {
        return runtimeType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void destroy() {
        try {

            if (streamStub != null) {
                streamStub.onCompleted();
            }

            if (streamStubAsBatch != null) {
                streamStubAsBatch.onCompleted();
            }

            if (!completionLatch.await(1, SECONDS)) {
                logger.info("gRPC call has not completed on time");
            }

            GrpcUtil.shutdownChannel(channel, logger, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void checkForServerError() {
        if (completionLatch.getCount() == 0) {
            throw new JetException("User Code Runtime [" + name + "] broke down: " + exceptionInOutputObserver,
                    exceptionInOutputObserver);
        }
    }

    /**
     * Observes list of message stream to the user code runtime.
     */
    private class OutputListOfMessageObserver implements StreamObserver<OutputListOfMessage> {
        @Override
        public void onNext(OutputListOfMessage outputItem) {
            try {
                List<Data> payloads = new ArrayList<>();

                for (ByteString item : outputItem.getPayLoadList()) {
                    payloads.add(new HeapData(item.toByteArray()));
                }

                futureBatchQueue.remove().complete(payloads);
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                e = GrpcUtil.translateGrpcException(e);

                exceptionInOutputObserver = e;
                for (CompletableFuture<List<Data>> future; (future = futureBatchQueue.poll()) != null; ) {
                    future.completeExceptionally(e);
                }
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            for (CompletableFuture<List<Data>> future; (future = futureBatchQueue.poll()) != null; ) {
                future.completeExceptionally(new JetException("Completion signaled before the future was completed"));
            }
            completionLatch.countDown();
        }
    }

    /**
     * Observes single message stream to the user code runtime.
     */
    private class OutputMessageObserver implements StreamObserver<OutputMessage> {
        @Override
        public void onNext(OutputMessage outputItem) {
            try {
                futureSingleQueue.remove().complete(new HeapData(outputItem.getPayLoad().toByteArray()));
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                e = GrpcUtil.translateGrpcException(e);
                exceptionInOutputObserver = e;
                for (CompletableFuture<Data> future; (future = futureSingleQueue.poll()) != null; ) {
                    future.completeExceptionally(e);
                }
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            for (CompletableFuture<Data> future; (future = futureSingleQueue.poll()) != null; ) {
                future.completeExceptionally(new JetException("Completion signaled before the future was completed"));
            }
            completionLatch.countDown();
        }
    }
}
