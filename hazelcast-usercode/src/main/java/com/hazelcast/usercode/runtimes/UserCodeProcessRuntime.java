package com.hazelcast.usercode.runtimes;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.usercode.UserCodeRuntimeBase;
import com.hazelcast.usercode.UserCodeService;
import com.hazelcast.usercode.UserCodeTransport;
import com.hazelcast.usercode.services.UserCodeProcess;

public class UserCodeProcessRuntime extends UserCodeRuntimeBase {

    private UserCodeProcess process;

    public UserCodeProcessRuntime(UserCodeService userCodeService, UserCodeTransport transport, SerializationService serializationService, UserCodeProcess process) {

        super(userCodeService, transport, serializationService);
        this.process = process;
        transport.setReceiver(this);
    }

    public void destroy() {
        process.destroy();
    }
}
