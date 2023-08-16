package com.hazelcast.usercode;

import com.hazelcast.logging.LoggingService;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public final class UserCodeServiceFactory {

    private static final Map<String, UserCodeService> services = new HashMap<>();

    private UserCodeServiceFactory() {
    }

    public static UserCodeService getService(String mode, LoggingService logging) {

        // TODO: do better
        // things should be in the same module, or properly injected, or whatever
        // and it's totally not thread safe
        // this is POC

        UserCodeService service = services.get(mode);
        if (service != null) {
            return service;
        }

        String className;
        switch (mode) {
            case "process":
                className = "com.hazelcast.usercode.services.UserCodeProcessService";
                break;
            case "container":
                className = "com.hazelcast.usercode.services.UserCodeContainerService";
                break;
            case "passthru":
                className = "com.hazelcast.usercode.services.UserCodePassThruService";
                break;
            default:
                throw new UserCodeException("Mode '" + mode + "' is not supported.");
        }

        try{
            Class<?> clazz = Class.forName(className);
            Constructor<?> ctor = clazz.getConstructor(LoggingService.class);
            service = (UserCodeService) ctor.newInstance(logging);
            services.put(mode, service);
            return service;
        }
        catch (Exception ex) {
            throw new UserCodeException("Failed to create the user-code service with mode '" + mode + "'.", ex);
        }
    }
}
