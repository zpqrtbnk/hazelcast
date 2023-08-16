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

import com.hazelcast.config.Config;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.usercoderuntime.impl.CreateRequest;
import com.hazelcast.jet.usercoderuntime.impl.CreateResponse;
import com.hazelcast.jet.usercoderuntime.impl.DeleteRequest;
import com.hazelcast.jet.usercoderuntime.impl.Empty;
import com.hazelcast.jet.usercoderuntime.impl.UserCodeRuntimeService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserCodeRuntimeServiceTest extends SimpleTestInClusterSupport {

    private Server server;
    private int TEST_PORT = 8081;
    private String TEST_ADDRESS = "127.0.0.1";
    private GrpcMockService mockService;
    private ProcessorSupplier.Context mockContext;

    @BeforeClass
    public static void beforeAll() {
        Config config = smallInstanceWithResourceUploadConfig();
        initialize(2, config);
    }

    @Before
    public void before() {

        mockContext = mock(ProcessorSupplier.Context.class);
        when(mockContext.hazelcastInstance()).thenReturn(instance());

        mockService = new GrpcMockService();
        server = ServerBuilder
                .forPort(TEST_PORT)
                .addService(mockService)
                .build();

        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        server.shutdownNow();
    }

    @Test
    public void userCodeRuntimeService_creates_grpcRuntime_test() {

        RuntimeServiceConfig config = new RuntimeServiceConfig();
        config.setControllerAddress(TEST_ADDRESS)
                .setControllerGrpcPort(TEST_PORT);
        UserCodeRuntimeService service = UserCodeRuntimeService.getInstance(config);

        String name = TEST_ADDRESS;

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setRuntimeType(RuntimeType.PYTHON)
                .setCommunicationType(RuntimeTransportType.GRPC)
                .setImageName("user/custom-image")
                .setPort(TEST_PORT);


        CreateRequest request = CreateRequest
                .newBuilder()
                .setImage(runtimeConfig.getImageName())
                .build();

        mockService.setCreateFn((req) -> {
            // Make sure service prepares proper request.
            Assert.assertEquals(request.getImage(), req.getImage());

            CreateResponse response = CreateResponse
                    .newBuilder()
                    .setName(name)
                    .build();

            return response;
        });

        UserCodeRuntime runtime = service.startRuntime(mockContext, runtimeConfig);

        Assert.assertEquals(runtime.getRuntimeType(), runtimeConfig.getRuntimeType());
        Assert.assertEquals(runtime.getName(), name);
        Assert.assertEquals(runtime.getTransportType(), RuntimeTransportType.GRPC);
        Assert.assertEquals(runtime.getRuntimeType(), RuntimeType.PYTHON);
    }

    @Test
    public void userCodeRuntimeService_destroys_runtime_test() throws InterruptedException, ExecutionException {

        RuntimeServiceConfig config = new RuntimeServiceConfig();
        config.setControllerAddress(TEST_ADDRESS)
                .setControllerGrpcPort(TEST_PORT);
        UserCodeRuntimeService service = UserCodeRuntimeService.getInstance(config);

        String name = "user_code_function";

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setImageName("user/image");

        DeleteRequest request = DeleteRequest
                .newBuilder()
                .setName(runtimeConfig.getImageName())
                .build();

        mockService.setDestroyFn((req) -> {
            Assert.assertEquals(name, req.getName());
            Empty response = Empty.newBuilder().build();

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return response;
        });

        Future f = service.destroyRuntimeAsync(name);

        while (!f.isDone()) {
        }

        Assert.assertNotNull(f.get());
    }
}
