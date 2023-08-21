package com.hazelcast.usercode.services;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.jobbuilder.InfoList;
import com.hazelcast.jet.jobbuilder.InfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeProcessRuntime;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;

// a UserCodeService that executes each runtime in a separate process
public final class UserCodeProcessService extends UserCodeServiceBase {

    private final ILogger logger;

    public UserCodeProcessService(LoggingService logging) {

        super(logging);
        this.logger = logging.getLogger(UserCodeProcessService.class);
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException {

        InfoMap processInfo = startInfo.childAsMap("process");

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

        InfoList argsInfo = processInfo.childAsList("args", false);
        int argsSize = 1 + (argsInfo == null ? 0 : argsInfo.size());
        String[] command = new String[argsSize];

        //logger.info("DEBUG: " + (argsInfo == null ? "args not found" : "found args"));
        //logger.info("DEBUG: command.length = " + command.length);

        command[0] = (processPath  == null ? "" : processPath + File.separator) + processName;
        //logger.info("DEBUG: command[0] = " + command[0]);

        if (argsInfo != null) {
            for (int i = 0; i < argsInfo.size(); i++) {
                String arg = argsInfo.itemAsString(i);
                command[i + 1] = startInfo.expand(arg, expand);
                //logger.info("DEBUG: args[" + i +"]='" + arg + "' -> command[" + (i+1) + "]=" + command[i+1]);
            }
        }

        // start the process
        logger.info("Start process " + String.join(" ", command));
        UserCodeProcess process = new UserCodeProcess(name, logging)
                .directory(directory)
                .command(command)
                .start();

        // create the transport
        UserCodeTransport transport = createTransport(startInfo);

        // create the runtime (which declares itself as a receiver of the transport)
        UserCodeProcessRuntime runtime = new UserCodeProcessRuntime(this, transport, serializationService, process);

        // initialize it all
        return initialize(runtime);
    }

    @Override
    public CompletableFuture<Void> destroyRuntime(UserCodeRuntime runtime) {

        if (!(runtime instanceof UserCodeProcessRuntime)) {
            throw new UnsupportedOperationException("runtime is not UserCodeProcessRuntime");
        }

        logger.info("destroy runtime");

        UserCodeProcessRuntime processRuntime = (UserCodeProcessRuntime) runtime;
        return terminate(processRuntime)
                .thenCompose(x -> {
                    processRuntime.destroy();
                    return CompletableFuture.completedFuture(null);
                });
    }
}
