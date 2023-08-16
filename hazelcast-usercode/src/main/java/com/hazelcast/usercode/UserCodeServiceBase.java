package com.hazelcast.usercode;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.transports.grpc.GrpcTransport;
import com.hazelcast.usercode.transports.sharedmemory.SharedMemoryTransport;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class UserCodeServiceBase implements UserCodeService, SerializationServiceAware {

    protected final LoggingService logging;
    protected SerializationService serializationService;

    public UserCodeServiceBase(LoggingService logging) {

        this.logging = logging;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    protected void ensureMode(String expected, UserCodeRuntimeStartInfo startInfo) {

        String mode = startInfo.get("mode");
        if (!expected.equals(mode)) {
            throw new UserCodeException("Cannot start a mode '" + mode + "' runtime, expecting mode '" + expected + "'.");
        }
    }

    protected UserCodeTransport createTransport(UserCodeRuntimeStartInfo startInfo) {

        // create the transport
        UserCodeTransport transport;
        String transportMode = startInfo.get("transport");
        switch (transportMode) {
            case "grpc":
                String address = startInfo.get("transport-address", "localhost");
                int port = startInfo.get("transport-port", 80);
                transport = new GrpcTransport(address, port, logging);
                break;
            case "shared-memory":
                UUID uniqueId = startInfo.get("uid");
                transport = new SharedMemoryTransport(uniqueId, logging);
                break;
            default:
                throw new UserCodeException("Unsupported transport mode '" + transportMode + "'.");
        }
        return transport;
    }

    public Future<Void> destroyRuntime(UserCodeRuntime runtime) {
        return CompletableFuture.completedFuture(null);
    }
}
