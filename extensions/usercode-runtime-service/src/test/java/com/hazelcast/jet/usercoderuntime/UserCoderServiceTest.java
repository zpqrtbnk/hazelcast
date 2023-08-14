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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.usercoderuntime.impl.RuntimeRequest;
import com.hazelcast.jet.usercoderuntime.impl.RuntimeResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class UserCoderServiceTest extends SimpleTestInClusterSupport {


    private ManagedChannel testChannel;
    private Server server;
    private int TEST_PORT = 8081;
    private String TEST_ADDRESS = "127.0.0.1";
    private GrpcMockService mockService;

    @Before
    public void before() {

        mockService = new GrpcMockService();
        server = ServerBuilder
                .forPort(TEST_PORT)
                .addService(mockService)
                .build();
    }

    @After
    public void after() {
        server.shutdownNow();
    }

    @Test
    public void userCodeServiceCreates_test() {

        UserCodeRuntimeServiceConfig config = new UserCodeRuntimeServiceConfig();
        config.setChannelFn((address, port) -> ManagedChannelBuilder.forAddress(TEST_ADDRESS, TEST_PORT));
        IUserCodeRuntimeService service = UserCodeRuntimeServiceFactory.create(null, config);

        String name = "user_code_function";
        int port = 1010;

        RuntimeConfig runtimeConfig = new RuntimeConfig();
        runtimeConfig.setRuntimeType(UserCodeRuntimeType.PYTHON);
        runtimeConfig.setCommunicationType(UserCodeCommunicationType.GRPC);
        runtimeConfig.setListeningPort(port);
        runtimeConfig.setImageName("user/custom-image");

        RuntimeRequest request = RuntimeRequest
                .newBuilder()
                .setImageName(runtimeConfig.getImageName())
                .setName(name)
                .setListeningPort(1010)
                .build();

        mockService.setCreateFn((req) -> {

            // Make sure service prepares proper request.
            Assert.assertEquals(request.getName(), req.getName());
            Assert.assertEquals(request.getListeningPort(), req.getListeningPort());
            Assert.assertEquals(request.getImageName(), req.getImageName());

            RuntimeResponse response = RuntimeResponse
                    .newBuilder()
                    .setAddress("1.1.1.1")
                    .setPort(request.getListeningPort())
                    .setUuid("abc-dce-123")
                    .build();

            return response;
        });

        IUserCodeRuntime runtime = service.startRuntime(request.getName(), runtimeConfig);
    }

}
