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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.util.concurrent.CompletableFuture;

// represents a UserCode runtime
public interface UserCodeRuntime  {

    // gets the SerializationService
    SerializationService getSerializationService();

    // gets the UserCodeService that created this runtime
    UserCodeService getUserCodeService();

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request object
    // returns: the response object
    CompletableFuture<?> invoke(String functionName, Object payload);

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request data
    // returns: the response data
    CompletableFuture<Data> invoke(String functionName, Data payload);

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request bytes
    // returns: the response bytes
    CompletableFuture<byte[]> invoke(String functionName, byte[] payload);
}
