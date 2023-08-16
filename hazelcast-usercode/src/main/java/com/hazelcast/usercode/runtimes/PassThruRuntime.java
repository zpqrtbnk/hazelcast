package com.hazelcast.usercode.runtimes;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.usercode.UserCodeRuntimeBase;
import com.hazelcast.usercode.UserCodeService;
import com.hazelcast.usercode.UserCodeTransport;

public class PassThruRuntime extends UserCodeRuntimeBase {

    public PassThruRuntime(UserCodeService userCodeService, UserCodeTransport transport, SerializationService serializationService) {
        super(userCodeService, transport, serializationService);
    }
}
