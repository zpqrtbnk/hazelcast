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

package com.hazelcast.usercode.services;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.PassThruRuntime;

import java.util.UUID;
import java.util.concurrent.Future;

public class UserCodePassThruService extends UserCodeServiceBase {

    private final ILogger logger;

    public UserCodePassThruService(String localMember, LoggingService logging) {

        super(localMember, logging);
        this.logger = logging.getLogger(UserCodeProcessService.class);
    }

    @Override
    public Future<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException {

        String passthruInfo = startInfo.childAsString("service");
        if (!passthruInfo.equals("passthru")) {
            throw new UserCodeException("panic");
        }

        // allocate the runtime unique identifier
        UUID uniqueId = UUID.randomUUID();
        startInfo.setChild("uid", uniqueId);

        // create the transport
        UserCodeTransport transport = createTransport(startInfo);

        // create the runtime
        PassThruRuntime runtime = new PassThruRuntime(this, transport, serializationService);

        // initialize it all
        return initialize(runtime);
    }
}
