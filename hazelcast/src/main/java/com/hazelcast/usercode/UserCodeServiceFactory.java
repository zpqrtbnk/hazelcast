package com.hazelcast.usercode;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LoggingService;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public final class UserCodeServiceFactory {

    private static final Map<String, UserCodeService> services = new HashMap<>();

    private UserCodeServiceFactory() {
    }

    public static UserCodeService getService(HazelcastInstance hazelcastInstance, String mode, LoggingService logging) {

        // TODO: do better, we're talking singletons here
        // and... we should multi-thread better and a cluster should support 1 unique service?

        UserCodeService service = services.get(mode);
        if (service != null) {
            return service;
        }

        // FIXME revisit format
        // FIXME getAddress().toString() returns [192.168.0.111]:5701 and what are these brackets?!
        Address memberAddress = hazelcastInstance.getCluster().getLocalMember().getAddress();
        String clusterName = hazelcastInstance.getConfig().getClusterName();
        String localMember = memberAddress.getHost() + ";" + memberAddress.getPort() + ";" + clusterName;

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

        // FIXME address and port of controller = member configuration properties
        String controllerAddress = "localhost";
        int controllerPort = 1234;

        try{
            Class<?> clazz = Class.forName(className);

            switch (mode) {
                case "process":
                case "passthru":
                    service = (UserCodeService) clazz
                            .getConstructor(String.class, LoggingService.class)
                            .newInstance(localMember, logging);
                    break;
                case "container":
                    service = (UserCodeService) clazz
                            .getConstructor(String.class, String.class, Integer.class, LoggingService.class)
                            .newInstance(localMember, controllerAddress, controllerPort, logging);
                    break;
            }
        }
        catch (Exception ex) {
            throw new UserCodeException("Failed to create the user-code service with mode '" + mode + "'.", ex);
        }

        services.put(mode, service);
        return service;
    }
}
