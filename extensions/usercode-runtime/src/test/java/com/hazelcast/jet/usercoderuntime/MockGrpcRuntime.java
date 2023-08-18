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

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.InputMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.JetToUserCodeRuntimeGrpc;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputListOfMessage;
import com.hazelcast.jet.usercoderuntime.impl.runtime.OutputMessage;
import com.hazelcast.logging.ILogger;
import io.grpc.stub.StreamObserver;

import java.util.function.Function;

/**
 * Mock of remote user coder runtime via grpc.
 */
public class MockGrpcRuntime extends JetToUserCodeRuntimeGrpc.JetToUserCodeRuntimeImplBase {

    private Function<InputMessage, OutputMessage> invokeFn;
    private Function<InputListOfMessage, OutputListOfMessage> invokeBatchFn;
    private ILogger logger;

    public MockGrpcRuntime(ProcessorSupplier.Context context) {
        this.logger = context.hazelcastInstance().getLoggingService()
                .getLogger(getClass().getPackage().getName());
    }

    public void setInvokeFn(Function<InputMessage, OutputMessage> invokeFn) {
        this.invokeFn = invokeFn;
    }

    public void setInvokeBatchFn(Function<InputListOfMessage, OutputListOfMessage> invokeBatchFn) {
        this.invokeBatchFn = invokeBatchFn;
    }

    @Override
    public StreamObserver<InputListOfMessage> invokeBatch(StreamObserver<OutputListOfMessage> responseObserver) {
        return new StreamObserver<InputListOfMessage>() {
            @Override
            public void onNext(InputListOfMessage value) {
                responseObserver.onNext(invokeBatchFn.apply(value));
            }

            @Override
            public void onError(Throwable t) {
                logger.severe("Error on invokeBatch: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<InputMessage> invoke(StreamObserver<OutputMessage> responseObserver) {
        return new StreamObserver<InputMessage>() {
            @Override
            public void onNext(InputMessage value) {
                responseObserver.onNext(invokeFn.apply(value));
            }

            @Override
            public void onError(Throwable t) {
                logger.severe("Error on invokeBatch: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
