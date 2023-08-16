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

import com.hazelcast.jet.usercoderuntime.impl.ControllerGrpc;
import com.hazelcast.jet.usercoderuntime.impl.CreateRequest;
import com.hazelcast.jet.usercoderuntime.impl.CreateResponse;
import com.hazelcast.jet.usercoderuntime.impl.DeleteRequest;
import com.hazelcast.jet.usercoderuntime.impl.Empty;
import io.grpc.stub.StreamObserver;
import java.util.function.Function;

public class GrpcMockService extends ControllerGrpc.ControllerImplBase {

    private Function<CreateRequest, CreateResponse> createFn;
    private Function<DeleteRequest, Empty> destroyFn;

    public void setCreateFn(Function<CreateRequest, CreateResponse> createFn) {
        this.createFn = createFn;
    }

    public void setDestroyFn(Function<DeleteRequest, Empty> destroyFn) {
        this.destroyFn = destroyFn;
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
        responseObserver.onNext(createFn.apply(request));
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(destroyFn.apply(request));
        responseObserver.onCompleted();
    }
}
