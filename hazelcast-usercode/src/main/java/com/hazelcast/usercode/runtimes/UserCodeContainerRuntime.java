package com.hazelcast.usercode.runtimes;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeRuntimeBase;
import com.hazelcast.usercode.UserCodeService;
import com.hazelcast.usercode.UserCodeTransport;

public class UserCodeContainerRuntime extends UserCodeRuntimeBase {

    private final String containerId;

    public UserCodeContainerRuntime(UserCodeService userCodeService, UserCodeTransport transport, SerializationService serializationService, String containerId) {

        super(userCodeService, transport, serializationService);
        this.containerId = containerId;
    }

    public String getContainerId() {
        return containerId;
    }
}
