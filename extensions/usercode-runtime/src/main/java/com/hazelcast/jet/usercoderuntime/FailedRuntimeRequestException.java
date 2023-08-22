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

import com.hazelcast.core.HazelcastException;

/**
 * Represents an exception that is thrown while processing a request to User Code Runtime Controller.
 */
public class FailedRuntimeRequestException extends HazelcastException {

    /**
     * Initialize an instance of {@link FailedRuntimeRequestException}
     */
    public FailedRuntimeRequestException() {
    }

    /**
     * Initialize an instance of {@link FailedRuntimeRequestException} with a string message.
     */
    public FailedRuntimeRequestException(String message) {
        super(message);
    }

    /**
     * Initialize an instance of {@link FailedRuntimeRequestException} with a string message and a {@link Throwable}
     */
    public FailedRuntimeRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Initialize an instance of {@link FailedRuntimeRequestException} with a {@link Throwable}
     */
    public FailedRuntimeRequestException(Throwable cause) {
        super(cause);
    }
}
