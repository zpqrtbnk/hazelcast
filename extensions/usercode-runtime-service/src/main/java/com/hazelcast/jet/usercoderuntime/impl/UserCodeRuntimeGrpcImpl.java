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


import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.usercoderuntime.IUserCodeRuntime;
import com.hazelcast.jet.usercoderuntime.UserCodeCommunicationType;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntimeContext;
import com.hazelcast.jet.usercoderuntime.UserCodeRuntimeType;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Implements the {@link IUserCodeRuntime} as grpc client.
 */
public class UserCodeRuntimeGrpcImpl implements IUserCodeRuntime {


    /**
     *
     * @param context
     */
    public UserCodeRuntimeGrpcImpl(UserCodeRuntimeContext context) { }

    @Override
    public Future<Data> invoke(Data payload) {
        return null;
    }

    @Override
    public Future<List<Data>> invoke(List<Data> payloads) {
        return null;
    }

    @Override
    public Future setRuntimeObject(Data payload) {
        return null;
    }

    @Override
    public UserCodeCommunicationType getCommunicationType() {
        return null;
    }

    @Override
    public UserCodeRuntimeType getRuntimeType() {
        return null;
    }


}
