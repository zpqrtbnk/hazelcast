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
import java.util.concurrent.Future;

/**
 * The client interface to call user function in user code runtime container.
 */
public interface IUserCodeRuntime {

    /**
     * Sends the payload to user function and returns the result of the invocation on user code runtime container.
     *
     * @param payload Serialized data to be passed to user function.
     * @return Result of invocation of the user function.
     * @since 5.4
     */
    Future<Data> invoke(Data payload);

    /**
     * Sends the list of payload to user function and returns the result of the invocation on user code runtime container.
     *
     * @param payloads Serialized list of data to be passed to user function.
     * @return Result of invocation of the user function.
     * @since 5.4
     */
    Future<List<Data>> invoke(List<Data> payloads);

    /**
     * Sets configuration object by passing given payload in the user code runtime.
     *
     * @param payload
     * @since 5.4
     * @return
     */
    Future setRuntimeObject(Data payload);

    /**
     * Gets communication type of between the instance and user code runtime container.
     *
     * @since 5.4
     */
    UserCodeCommunicationType getCommunicationType();

    /**
     * Gets runtime type of the user code runtime container.
     *
     * @since 5.4
     */
    UserCodeRuntimeType getRuntimeType();
}

