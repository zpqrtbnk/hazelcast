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

package com.hazelcast.usercode.transports;

import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransport;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class MultiplexTransportBase implements UserCodeTransport {

    // FIXME extract constants
    private final int timeout = 10;
    private final TimeUnit timeoutUnit = TimeUnit.SECONDS;

    private final Map<Long, CompletableFuture<UserCodeMessage>> futures = new HashMap<>();
    private final UserCodeMessage timeoutMessage = new UserCodeMessage(0, ".TIMEOUT", null);

    protected CompletableFuture<UserCodeMessage> createFuture(UserCodeMessage message) {

        CompletableFuture<UserCodeMessage> future = new CompletableFuture<UserCodeMessage>().completeOnTimeout(timeoutMessage, timeout, timeoutUnit);
        futures.put(message.getId(), future);
        return future;
    }

    protected void completeFuture(UserCodeMessage message) {

        CompletableFuture<UserCodeMessage> future = futures.remove(message.getId());
        if (future != null) {
            future.complete(message);
        }
    }

    protected void failFuture(UserCodeMessage message, Throwable t) {

        CompletableFuture<UserCodeMessage> future = futures.remove(message.getId());
        if (future != null) {
            future.completeExceptionally(t);
        }
    }

    protected void failFutures(Throwable t) {

        for (Map.Entry<Long, CompletableFuture<UserCodeMessage>> entry : futures.entrySet()) {
            entry.getValue().completeExceptionally(t);
        }
    }
}
