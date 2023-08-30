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

package com.hazelcast.usercode.transports.grpc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.usercode.UserCodeException;
import io.grpc.ManagedChannel;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class GrpcUtil {

    private GrpcUtil() {
    }

    /**
     * {@link io.grpc.StatusException} and {@link io.grpc.StatusRuntimeException}
     * break the Serializable contract, see
     * <a href="https://github.com/grpc/grpc-java/issues/1913">gRPC Issue #1913</a>.
     * This method replaces them with serializable ones.
     *
     * @param exception the exception to examine and possibly replace
     * @return the same exception or a replacement if needed
     */
    public static Throwable translateGrpcException(Throwable exception) {
        // FIXME what's this and maybe we should "just" depend on the hazelcast grpc package instead of duplicating?!
        if (exception instanceof StatusException) {
            //return new StatusExceptionJet((StatusException) exception);
            return new UserCodeException("StatusException: " + exception.getMessage(), exception);
        } else if (exception instanceof StatusRuntimeException) {
            //return new StatusRuntimeExceptionJet((StatusRuntimeException) exception);
            return new UserCodeException("StatusRuntimeException: " + exception.getMessage(), exception);
        }
        return exception;
    }

    /**
     * Shuts down a {@link ManagedChannel}
     * <p>
     * Tries orderly shutdown first {@link ManagedChannel#shutdown()}, then forceful shutdown
     * {@link ManagedChannel#shutdownNow()}.
     */
    public static void shutdownChannel(ManagedChannel channel, ILogger logger, long timeout) throws InterruptedException {
        if (!channel.shutdown().awaitTermination(timeout, SECONDS)) {
            logger.info("gRPC client has not shut down within " + timeout + " seconds, you can override the timeout " +
                    "by setting the `jet.grpc.shutdown.timeout.seconds` system property");

            if (!channel.shutdownNow().awaitTermination(1, SECONDS)) {
                logger.info("gRPC client has not shut down on time, even after forceful shutdown");
            }
        }
    }
}
