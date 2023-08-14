package com.hazelcast.usercode;

import com.hazelcast.logging.LoggingService;

import java.lang.reflect.Constructor;

public final class UserCodeServiceFactory {

    private UserCodeServiceFactory() {
    }

    public static UserCodeService getService(LoggingService logging) {

        try {
            // the actual service type should be configured somehow at member (cluster) level
            // we don't expect a member (cluster) to support multiple services at once
            Class<?> clazz = Class.forName("com.hazelcast.usercode.services.UserCodeProcessService");
            Constructor<?> ctor = clazz.getConstructor(LoggingService.class);
            UserCodeService service = (UserCodeService) ctor.newInstance(logging);
            return service;
        }
        catch (Exception ex) {
            throw new UserCodeException("Failed to create the user-code service.", ex);
        }
    }
}
