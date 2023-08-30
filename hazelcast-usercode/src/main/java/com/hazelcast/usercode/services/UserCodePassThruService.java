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
