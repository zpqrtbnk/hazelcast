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

import com.hazelcast.internal.serialization.Data;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The client interface to call user function in user code runtime container.
 */
public interface UserCodeRuntime {

    /**
     * Sends the payload to user function and returns the result of the invocation on user code runtime container.
     *
     * @param payload Serialized data to be passed to user function.
     * @return Result of invocation of the user function.
     * @since 5.4
     */
    CompletableFuture<Data> invoke(Data payload);

    /**
     * Sends the list of payload to user function and returns the result of the invocation on user code runtime container.
     *
     * @param payloads Serialized list of data to be passed to user function.
     * @return Results of invocation of the user function.
     * @since 5.4
     */
    CompletableFuture<List<Data>> invoke(List<Data> payloads);

    /**
     * Gets communication type of between the instance and user code runtime container.
     *
     * @since 5.4
     */
    RuntimeTransportType getTransportType();

    /**
     * Gets runtime type of the user code runtime container.
     *
     * @since 5.4
     */
    RuntimeType getRuntimeType();

    /**
     * Gets the cluster-wide unique name of the user code runtime. The name can be used as address in K8s.
     *
     * @since 5.4
     */
    String getName();

    /**
     * Destroys ONLY the object and releases the resources. To kill the runtime,
     * {@code  com.hazelcast.jet.usercoderuntime.impl.UserCodeRuntimeService.destroyRuntimeAsync()} should be invoked.
     *
     * @since 5.4
     */
    void destroy();
}

