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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.jobbuilder.JobBuilderInfoList;
import com.hazelcast.jet.jobbuilder.JobBuilderInfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeProcessRuntime;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.CompletableFuture;

// a UserCodeService that executes each runtime in a separate process
public final class UserCodeProcessService extends UserCodeServiceBase {

    private final ILogger logger;

    public UserCodeProcessService(String localMember, LoggingService logging) {

        super(localMember, logging);
        this.logger = logging.getLogger(UserCodeProcessService.class);
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException {

        JobBuilderInfoMap processInfo = startInfo.childAsMap("service").childAsMap("process");

        // allocate the runtime unique identifier
        UUID uniqueId = UUID.randomUUID();
        startInfo.setChild("uid", uniqueId);

        Map<String, String> expand = new HashMap<>();
        expand.put("UID", uniqueId.toString());
        expand.put("PLATFORM", startInfo.getPlatform());

        // run <processPath>/<processName> in <directory>
        String processName = processInfo.childAsString("name");
        String processPath = processInfo.childAsString("path", false);
        if (processPath != null) processPath = startInfo.expand(processPath, expand);
        String directory = processInfo.childAsString("work-directory", false);
        if (directory != null) directory = startInfo.expand(directory, expand);
        else directory = processPath == null ? System.getProperty("user.dir") : processPath;

        String command0 = (processPath  == null ? "" : processPath + File.separator) + processName;

        JobBuilderInfoList argsInfo = processInfo.childAsList("args", false);
        int argsSize = 1 + (argsInfo == null ? 0 : argsInfo.size());
        String[] command = new String[argsSize];

        command[0] = command0;

        if (argsInfo != null) {
            for (int i = 0; i < argsInfo.size(); i++) {
                String arg = argsInfo.itemAsString(i);
                command[i + 1] = startInfo.expand(arg, expand);
            }
        }

        // ensure that the executable file exists
        Path command0path = Paths.get(command0);
        if (!Files.exists(command0path)) {
            throw new UserCodeException("File not found: " + command0);
        }

        // on some OS the file actually needs to be executable
        if (!OsHelper.isWindows()) {
            try {
                Set<PosixFilePermission> perms = Files.getPosixFilePermissions(command0path);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                Files.setPosixFilePermissions(command0path, perms);
            }
            catch (IOException ex) {
                throw new UserCodeException("Failed to chmod u+x process.", ex);
            }
        }

        // start the process
        logger.info("Start process " + String.join(" ", command) + " in directory " + directory);
        UserCodeProcess process = new UserCodeProcess(name, logging)
                .directory(directory)
                .command(command)
                .start();

        // create the transport
        UserCodeTransport transport = createTransport(startInfo);

        // create the runtime
        UserCodeProcessRuntime runtime = new UserCodeProcessRuntime(this, transport, serializationService, process);

        // initialize it all
        return initialize(runtime);
    }

    @Override
    public CompletableFuture<Void> destroyRuntime(UserCodeRuntime runtime) {

        if (!(runtime instanceof UserCodeProcessRuntime)) {
            throw new UnsupportedOperationException("runtime is not UserCodeProcessRuntime");
        }

        UserCodeProcessRuntime processRuntime = (UserCodeProcessRuntime) runtime;
        return terminate(processRuntime)
                .thenCompose(x -> {
                    processRuntime.destroy();
                    return CompletableFuture.completedFuture(null);
                });
    }
}
