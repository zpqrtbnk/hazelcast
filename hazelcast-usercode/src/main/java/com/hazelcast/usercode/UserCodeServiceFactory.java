package com.hazelcast.usercode;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.services.UserCodeContainerService;
import com.hazelcast.usercode.services.UserCodePassThruService;
import com.hazelcast.usercode.services.UserCodeProcessService;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public final class UserCodeServiceFactory {

    private static final Map<String, UserCodeService> services = new HashMap<>();
    private static final Object mutex = new Object();

    // TODO: singleton?
    //   we're making this class a static singleton, is there
    //   a better way to register a member-level singleton?

    private UserCodeServiceFactory() {
    }

    public static UserCodeService getService(HazelcastInstance hazelcastInstance, String mode, LoggingService logging) {

        synchronized (mutex) {
            UserCodeService service = services.get(mode);
            if (service == null) {
                service = createService(hazelcastInstance, mode, logging);
                services.put(mode, service);
            }
            return service;
        }
    }

    private static UserCodeService createService(HazelcastInstance hazelcastInstance, String mode, LoggingService logging) {

        // beware getAddress().toString() returns [192.168.0.111]:5701 and, brackets?!
        Address memberAddress = hazelcastInstance.getCluster().getLocalMember().getAddress();
        String clusterName = hazelcastInstance.getConfig().getClusterName();
        String localMember = memberAddress.getHost() + ";" + memberAddress.getPort() + ";" + clusterName;

        switch (mode) {

            case "process":
                return new UserCodeProcessService(localMember, logging);

            case "passthru":
                return new UserCodePassThruService(localMember, logging);

            case "container":
                // FIXME address and port of controller should be member configuration properties
                String controllerAddress = System.getenv("HZ_RUNTIME_CONTROLLER_ADDRESS");
                if (controllerAddress == null) {
                    controllerAddress = "localhost";
                }
                String controllerPortString = System.getenv("HZ_RUNTIME_CONTROLLER_PORT");
                if (controllerPortString == null) {
                    controllerPortString = "55555";
                }
                int controllerPort = Integer.parseInt(controllerPortString);
                return new UserCodeContainerService(localMember, controllerAddress, controllerPort, logging);

            default:
                throw new UserCodeException("Unknown runtime '" + mode + "'.");
        }
    }
}
