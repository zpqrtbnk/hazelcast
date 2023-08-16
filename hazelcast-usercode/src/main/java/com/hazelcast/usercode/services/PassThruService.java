package com.hazelcast.usercode.services;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;
import com.hazelcast.usercode.runtimes.PassThruRuntime;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class PassThruService extends UserCodeServiceBase {

    private final ILogger logger;

    public PassThruService(LoggingService logging) {

        super(logging);
        this.logger = logging.getLogger(UserCodeProcessService.class);
    }

    @Override
    public Future<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeStartInfo startInfo) throws UserCodeException {

        ensureMode("passthru", startInfo);
        UserCodeTransport transport = createTransport(startInfo);
        UserCodeRuntime runtime = new PassThruRuntime(this, transport, serializationService);
        return CompletableFuture.completedFuture(runtime);
    }
}
