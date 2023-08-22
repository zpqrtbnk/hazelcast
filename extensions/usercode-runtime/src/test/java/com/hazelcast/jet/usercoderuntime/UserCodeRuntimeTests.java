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

package com.hazelcast.jet.usercoderuntime;

import com.google.protobuf.ByteString;
import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.usercoderuntime.impl.UserCodeRuntimeService;
import com.hazelcast.jet.usercoderuntime.impl.controller.CreateRequest;
import com.hazelcast.jet.usercoderuntime.impl.controller.CreateResponse;
import com.hazelcast.jet.usercoderuntime.impl.controller.Empty;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputMessage;
import com.hazelcast.logging.ILogger;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserCodeRuntimeTests extends SimpleTestInClusterSupport {
    private static final int CONTROLLER_PORT = 8081;
    private static final int RUNTIME_PORT = 8082;
    private static final String TEST_ADDRESS = "127.0.0.1";
    private Server server;
    private MockGrpcServerController mockControllerService;
    private ProcessorSupplier.Context mockContext;
    private Server runtimeRemoteServer;

    @BeforeClass
    public static void beforeAll() {
        Config config = smallInstanceWithResourceUploadConfig();
        initialize(2, config);

        RuntimeServiceConfig serviceConfig = new RuntimeServiceConfig();
        serviceConfig.setControllerAddress(TEST_ADDRESS)
                .setControllerGrpcPort(CONTROLLER_PORT);
        UserCodeRuntimeService.getInstance(serviceConfig);
    }

    @Before
    public void before() {

        mockContext = mock(ProcessorSupplier.Context.class);
        when(mockContext.hazelcastInstance()).thenReturn(instance());

        CreateGrpcServer();

        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void CreateGrpcServer() {
        mockControllerService = new MockGrpcServerController();
        server = ServerBuilder
                .forPort(CONTROLLER_PORT)
                .addService(mockControllerService)
                .build();
    }

    @After
    public void after() throws InterruptedException {
        server.shutdownNow().awaitTermination();
        if (runtimeRemoteServer != null) {
            runtimeRemoteServer.shutdownNow().awaitTermination();
        }
    }

    @Test
    public void userCodeRuntimeService_creates_grpcRuntime_test() {
        String name = TEST_ADDRESS;

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setRuntimeType(RuntimeType.PYTHON)
                .setCommunicationType(RuntimeTransportType.GRPC)
                .setImageName("user/custom-image")
                .setPort(CONTROLLER_PORT);


        CreateRequest request = CreateRequest
                .newBuilder()
                .setImage(runtimeConfig.getImageName())
                .build();

        mockControllerService.setCreateFn((req) -> {
            // Make sure service prepares proper request.
            Assert.assertEquals(request.getImage(), req.getImage());

            CreateResponse response = CreateResponse
                    .newBuilder()
                    .setName(name)
                    .build();

            return response;
        });

        UserCodeRuntime runtime = UserCodeRuntimeService.getInstance().startRuntime(mockContext, runtimeConfig);

        Assert.assertEquals(runtime.getRuntimeType(), runtimeConfig.getRuntimeType());
        Assert.assertEquals(runtime.getName(), name);
        Assert.assertEquals(runtime.getTransportType(), RuntimeTransportType.GRPC);
        Assert.assertEquals(runtime.getRuntimeType(), RuntimeType.PYTHON);
    }

    @Test
    public void userCodeRuntimeService_destroys_runtime_test() throws InterruptedException, ExecutionException {

        String name = "user_code_function";

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setImageName("user/image");

        mockControllerService.setDestroyFn((req) -> {
            Assert.assertEquals(name, req.getName());
            Empty response = Empty.newBuilder().build();

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return response;
        });

        Future f = UserCodeRuntimeService.getInstance().destroyRuntimeAsync(name);

        Assert.assertNotNull(f.get());
    }

    @Test(timeout = 15_000)
    public void userCodeRuntimeService_retry_destroys_when_controller_restart_test()
            throws InterruptedException,
            ExecutionException,
            IOException {

        String name = "user_code_function";

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setImageName("user/image");

        final CountDownLatch latchStartServer = new CountDownLatch(1);
        final CountDownLatch latchAsserted = new CountDownLatch(1);

        //Define destroy function on the server.
        mockControllerService.setDestroyFn((req) -> {
            Assert.assertEquals(name, req.getName());
            Empty response = Empty.newBuilder().build();
            return response;
        });

        // Initialize the service and let it connect to grpc server.
        UserCodeRuntimeService.getInstance();

        server.shutdownNow().awaitTermination();

        Runnable restartServer = new Runnable() {
            @Override
            public void run() {
                try {
                    CreateGrpcServer();
                    server.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable destroyRuntime = new Runnable() {
            @Override
            public void run() {
                // It will wait for a while until connection reestablished.
                // We cannot re-try at the background because we don't have a thread to watch out the invocation.
                Future f = UserCodeRuntimeService.getInstance().destroyRuntimeAsync(name);
                try {
                    Assert.assertNotNull(f.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    latchAsserted.countDown();
                }
            }
        };
        destroyRuntime.run();
        restartServer.run();
        latchAsserted.await();
    }

    @Test
    public void userCodeRuntime_does_invoke_test() throws IOException, ExecutionException, InterruptedException {

        mockControllerService.setCreateFn(createRequest -> {
            return CreateResponse.newBuilder()
                    .setName(TEST_ADDRESS)
                    .build();
        });
        ILogger logger = mockContext.hazelcastInstance().getLoggingService().getLogger(UserCodeRuntimeTests.class);

        // Prepare a grpc server to mock remote user code runtime.
        runtimeRemoteServer = getRuntimeRemoteGrpcServer(
                (inputMessage -> {
                    // Data is being processed on the user runtime.
                    Data d = markReplied(inputMessage.getPayLoad());

                    OutputMessage out = OutputMessage
                            .newBuilder()
                            .setPayLoad(ByteString.copyFrom(d.toByteArray()))
                            .build();

                    return out;
                }), null
        );

        runtimeRemoteServer.start();

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setImageName(TEST_ADDRESS)
                .setPort(RUNTIME_PORT);

        // Runtime is started
        UserCodeRuntime runtime = UserCodeRuntimeService
                .getInstance()
                .startRuntime(mockContext, runtimeConfig);

        Queue<CompletableFuture<Data>> futures = new ArrayDeque<>();

        int dataCount = 1000;
        for (int i = 0; i < dataCount; i++) {

            String m = "user-data-" + i;
            Data d = new HeapData(m.getBytes(StandardCharsets.UTF_8));
            futures.add(runtime.invoke(d));
        }

        int i = 0;
        while (!futures.isEmpty()) {

            Optional<CompletableFuture<Data>> f = futures
                    .stream().filter(listCompletableFuture -> listCompletableFuture.isDone()).findFirst();

            if (f.isEmpty()) {
                continue;
            }

            // Check whether order is preserved.
            Assert.assertEquals(f.get(), futures.peek());

            Data d = futures.poll().get();

            String m = new String(d.toByteArray(), StandardCharsets.UTF_8);
            String expected = "user-data-" + i++ + "-REPLIED";
            Assert.assertEquals(expected, m);
        }

        Assert.assertEquals(dataCount, i);
    }

    private Data markReplied(ByteString inputMessage) {
        String m = new String(inputMessage.toByteArray(), StandardCharsets.UTF_8);
        m += "-REPLIED";
        Data d = new HeapData(m.getBytes(StandardCharsets.UTF_8));
        return d;
    }

    @Test
    public void userCodeRuntime_does_invokeBatch_test() throws IOException, ExecutionException, InterruptedException {

        mockControllerService.setCreateFn(createRequest -> {
            return CreateResponse.newBuilder()
                    .setName(TEST_ADDRESS)
                    .build();
        });
        ILogger logger = mockContext.hazelcastInstance().getLoggingService().getLogger(UserCodeRuntimeTests.class);

        // Prepare a grpc server to mock remote user code runtime.
        runtimeRemoteServer = getRuntimeRemoteGrpcServer(null,
                (inputListOfMessage -> {
                    // Data is being processed on the user runtime.
                    List<ByteString> messages = inputListOfMessage.getPayLoadList();
                    OutputListOfMessage.Builder outBuilder = OutputListOfMessage.newBuilder();

                    for (ByteString m : messages) {
                        Data d = markReplied(m);
                        outBuilder.addPayLoad(ByteString.copyFrom(d.toByteArray()));
                    }

                    return outBuilder.build();
                })
        );

        runtimeRemoteServer.start();

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setImageName(TEST_ADDRESS)
                .setPort(RUNTIME_PORT);

        // Runtime is started
        UserCodeRuntime runtime = UserCodeRuntimeService.getInstance().startRuntime(mockContext, runtimeConfig);

        Queue<CompletableFuture<List<Data>>> futures = new ArrayDeque<>();

        List<Data> batchOfData = new ArrayList<>();

        int dataCount = 1000;
        int batchSize = 10;
        for (int i = 0; i < dataCount; i++) {

            String m = "user-data-" + i;
            Data d = new HeapData(m.getBytes(StandardCharsets.UTF_8));
            batchOfData.add(d);

            // Send batch
            if (i > 0 && (i % batchSize == 0 || dataCount - i == 1)) { // don't miss the last batch.
                futures.add(runtime.invoke(batchOfData));
                batchOfData.clear();
            }
        }

        Assert.assertEquals(dataCount / batchSize, futures.size());

        int i = 0;
        while (!futures.isEmpty()) {

            Optional<CompletableFuture<List<Data>>> f = futures
                    .stream().filter(listCompletableFuture -> listCompletableFuture.isDone()).findFirst();

            if (f.isEmpty()) {
                continue;
            }

            // Check whether order is preserved.
            Assert.assertEquals(f.get(), futures.peek());

            List<Data> dataList = futures.poll().get();

            for (Data d : dataList) {
                String m = new String(d.toByteArray(), StandardCharsets.UTF_8);
                String expected = "user-data-" + i++ + "-REPLIED";
                Assert.assertEquals(expected, m);
            }
        }

        Assert.assertEquals(dataCount, i);
    }

    private Server getRuntimeRemoteGrpcServer(Function<InputMessage, OutputMessage> invokeFn,
                                              Function<InputListOfMessage, OutputListOfMessage> invokeBatchFn) {

        MockGrpcRuntime runtimeRemoteService = new MockGrpcRuntime(mockContext);
        runtimeRemoteService.setInvokeBatchFn(invokeBatchFn);
        runtimeRemoteService.setInvokeFn(invokeFn);

        Server runtimeRemoteServer = ServerBuilder
                .forPort(RUNTIME_PORT)
                .addService(runtimeRemoteService)
                .build();

        return runtimeRemoteServer;
    }

}
