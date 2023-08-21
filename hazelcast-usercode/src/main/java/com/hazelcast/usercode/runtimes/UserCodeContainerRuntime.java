package com.hazelcast.usercode.runtimes;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeRuntimeBase;
import com.hazelcast.usercode.UserCodeService;
import com.hazelcast.usercode.UserCodeTransport;

public class UserCodeContainerRuntime extends UserCodeRuntimeBase {

    private final String containerName;

    public UserCodeContainerRuntime(UserCodeService userCodeService, UserCodeTransport transport, SerializationService serializationService, String containerName) {

        super(userCodeService, transport, serializationService);
        this.containerName = containerName;
    }

    public String getContainerName() {
        return containerName;
    }
}
