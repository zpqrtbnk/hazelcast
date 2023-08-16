package com.hazelcast.usercode.services;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeProcessRuntime;
import com.hazelcast.usercode.transports.grpc.GrpcTransport;
import com.hazelcast.usercode.transports.sharedmemory.SharedMemoryTransport;

import java.io.File;
import java.util.UUID;
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
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeStartInfo startInfo) throws UserCodeException {

        ensureMode("process", startInfo);

        // allocate the runtime unique identifier
        UUID uniqueId = UUID.randomUUID();
        startInfo.set("uid", uniqueId);

        // run <processPath>/<processName> in <directory>
        String processName = startInfo.get("process-name");
        String processPath = startInfo.get("process-path");
        if (processPath.startsWith("@")) {
            String resourceId = processPath.substring(1);
            processPath = startInfo.recreateResourceDirectory(resourceId);
        }
        String directory = startInfo.get("directory", processPath);

        // start the process
        logger.info("Start process " + processPath + File.separator + processName);
        UserCodeProcess process = new UserCodeProcess(name, logging)
                .directory(directory)
                .command(processPath + File.separator + processName, uniqueId.toString())
                .start();

        // create the transport
        UserCodeTransport transport = createTransport(startInfo);

        // create the runtime (which declares itself as a receiver of the transport)
        UserCodeProcessRuntime runtime = new UserCodeProcessRuntime(this, transport, serializationService, process);
        transport.open(); // FIXME could this be async? should this be runtime.connect()? runtime.connectTransport()?
        return CompletableFuture.completedFuture(runtime);
    }

    @Override
    public CompletableFuture<Void> destroyRuntime(UserCodeRuntime runtime) {

        if (!(runtime instanceof UserCodeProcessRuntime)) {
            throw new UnsupportedOperationException("runtime is not UserCodeProcessRuntime");
        }

        UserCodeProcessRuntime processRuntime = (UserCodeProcessRuntime) runtime;
        processRuntime.destroy();
        return CompletableFuture.completedFuture(null);
    }
}
