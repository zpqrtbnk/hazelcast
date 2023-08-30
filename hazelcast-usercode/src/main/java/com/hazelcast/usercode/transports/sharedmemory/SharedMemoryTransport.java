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

package com.hazelcast.usercode.transports.sharedmemory;

import com.hazelcast.jet.jobbuilder.JobBuilderInfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.UserCodeException;
import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.transports.MultiplexTransportBase;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// a UserCodeTransport that works over shared memory
public final class SharedMemoryTransport extends MultiplexTransportBase implements UserCodeTransportReceiver {

    // FIXME should be parameters & options
    private final int DATA_CAPACITY = 1024; // bytes
    private final int SPIN_DELAY = 4; // ms
    private final int OPEN_TIMEOUT = 5000; // ms

    private final UUID uniqueId;
    private final ILogger logger;
    private SharedMemoryPipe pipe;

    public SharedMemoryTransport(JobBuilderInfoMap transportInfo, LoggingService logging) {

        this.uniqueId = transportInfo.childAsUUID("uid");
        this.logger = logging.getLogger(SharedMemoryTransport.class);
    }

    @Override
    public CompletableFuture<Void> open() {

        // by convention the runtime is supposed to create a shared memory file at /temp/hazelcast-shm-<uniqueId>
        Path filepath = Paths.get(System.getProperty("java.io.tmpdir"), "hazelcast-shm-" + uniqueId);
        logger.info("Open SharedMemory transport (" + filepath + ")");
        pipe = new SharedMemoryPipe(uniqueId, filepath, this, DATA_CAPACITY, SPIN_DELAY, OPEN_TIMEOUT);

        // shared memory pipe will try to connect for some time before giving up w/ UserCodeException
        pipe.open();

        logger.info("Opened SharedMemory transport (" + filepath + ")");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<UserCodeMessage> invoke(UserCodeMessage message) {

        logger.info("SEND " + message.getId() + " " + message.getFunctionName());
        pipe.write(message);
        return createFuture(message);
    }

    @Override
    public void receive(UserCodeMessage message) {

        logger.info("RECV " + message.getId() + " " + message.getFunctionName());
        completeFuture(message);
    }

    @Override
    public void destroy() {

        pipe.destroy();
        failFutures(new UserCodeException("Transport closed."));
    }
}

