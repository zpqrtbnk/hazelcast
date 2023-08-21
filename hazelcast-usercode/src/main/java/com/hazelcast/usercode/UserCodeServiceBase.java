package com.hazelcast.usercode;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.jobbuilder.InfoMap;
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

    //protected void ensureMode(String expected, UserCodeRuntimeStartInfo startInfo) {
    //
    //    String mode = startInfo.info().("mode");
    //    if (!expected.equals(mode)) {
    //        throw new UserCodeException("Cannot start a mode '" + mode + "' runtime, expecting mode '" + expected + "'.");
    //    }
    //}

    protected UserCodeTransport createTransport(UserCodeRuntimeInfo runtimeInfo) {

        // create the transport
        String transportName;
        InfoMap transportInfo;
        if (runtimeInfo.childIsString("transport")) {
            transportName = runtimeInfo.childAsString("transport");
            transportInfo = new InfoMap();
        }
        else {
            InfoMap info = runtimeInfo.childAsMap("transport");
            transportName = info.uniqueChildName();
            transportInfo = info.childAsMap(transportName);
        }

        UUID uniqueId = runtimeInfo.childAsUUID("uid");
        transportInfo.setChild("uid", uniqueId);

        UserCodeTransport transport;
        switch (transportName) {
            case "grpc":
                transport = new GrpcTransport(transportInfo, logging);
                break;
            case "shared-memory":
                transport = new SharedMemoryTransport(transportInfo, logging);
                break;
            default:
                throw new UserCodeException("Unsupported transport mode '" + transportName + "'.");
        }
        return transport;
    }

    protected CompletableFuture<UserCodeRuntime> initialize(UserCodeRuntimeBase runtime) {

        runtime.getTransport().open(); // FIXME could this be async?

        return runtime.getTransport()
                .invoke(new UserCodeMessage(0, ".CONNECT", new byte[0]))
                .thenApply(response -> {
                    if (response.isError()) {
                        throw new UserCodeException("Exception in .CONNECT: " + response.getErrorMessage());
                    }
                    return runtime;
                });
    }

    protected CompletableFuture<Void> terminate(UserCodeRuntimeBase runtime) {

        return runtime.getTransport()
                .invoke(new UserCodeMessage(0, ".END", new byte[0]))
                .thenApply(response -> {
                    if (response.isError()) {
                        throw new UserCodeException("Exception in .END: " + response.getErrorMessage());
                    }
                    return null;
                });
    }

    public Future<Void> destroyRuntime(UserCodeRuntime runtime) {
        return CompletableFuture.completedFuture(null);
    }
}
