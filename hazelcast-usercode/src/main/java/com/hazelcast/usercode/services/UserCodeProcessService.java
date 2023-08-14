package com.hazelcast.usercode.services;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.UserCodeProcessRuntime;
import com.hazelcast.usercode.transports.grpc.UserCodeGrpcTransport;
import com.hazelcast.usercode.transports.sharedmemory.SharedMemoryTransport;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// a UserCodeService that executes each runtime in a separate process
public final class UserCodeProcessService implements UserCodeService, SerializationServiceAware {

    private final LoggingService logging;
    private SerializationService serializationService;

    public UserCodeProcessService(LoggingService logging) {

        this.logging = logging;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public CompletableFuture<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeStartInfo startInfo) throws UserCodeException {

        String mode = startInfo.get("mode");
        if (!"process".equals(mode)) {
            throw new UserCodeException("Cannot start a mode '" + mode + "' runtime, expecting mode 'process'.");
        }

        // allocate the runtime unique identifier
        UUID uniqueId = UUID.randomUUID();

        // run <processPath>/<processName> in <directory>
        String processName = startInfo.get("process-name");
        String processPath = startInfo.get("process-path");
        if (processPath.startsWith("@")) {
            String resourceId = processPath.substring(1);
            processPath = startInfo.recreateResourceDirectory(resourceId);
        }
        String directory = startInfo.get("directory", processPath);

        // start the process
        UserCodeProcess process = new UserCodeProcess(name, logging)
                .directory(directory)
                .command(processPath + File.separator + processName, uniqueId.toString())
                .start();

        // create the transport
        UserCodeTransport transport;
        String transportMode = startInfo.get("transport");
        switch (transportMode) {
            case "grpc":
                int port = startInfo.get("transport-port", 80);
                transport = new UserCodeGrpcTransport("localhost", port);
                break;
            case "shared-memory":
                transport = new SharedMemoryTransport(uniqueId);
                break;
            default:
                throw new UserCodeException("Unsupported transport mode '" + transportMode + "'.");
        }

        // create the runtime (which declares itself as a receiver of the transport)
        UserCodeProcessRuntime runtime = new UserCodeProcessRuntime(this, transport, serializationService, process);
        transport.open(); // FIXME could this be async? should this be runtime.connect()?
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
