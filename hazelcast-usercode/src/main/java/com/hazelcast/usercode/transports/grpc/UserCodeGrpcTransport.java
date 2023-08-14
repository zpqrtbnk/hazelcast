package com.hazelcast.usercode.transports.grpc;

//import com.hazelcast.usercode.grpc.*;

import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransport;
import com.hazelcast.usercode.UserCodeTransportReceiver;

import java.util.concurrent.CompletableFuture;

// a UserCodeTransport that works over gRPC
public final class UserCodeGrpcTransport implements UserCodeTransport {

    private final String address;
    private final int port;

    public UserCodeGrpcTransport(String address, int port) {

        this.address = address;
        this.port = port;
    }

    @Override
    public void setReceiver(UserCodeTransportReceiver receiver) {

    }

    @Override
    public CompletableFuture<Void> open() {
        return null;
    }

    @Override
    public CompletableFuture<Void> send(UserCodeMessage message) {
        return null;
    }

    @Override
    public void destroy() {

    }
}
