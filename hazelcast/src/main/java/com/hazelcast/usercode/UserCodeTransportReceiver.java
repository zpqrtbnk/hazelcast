package com.hazelcast.usercode;

// represents a class that can receive transport messages
public interface UserCodeTransportReceiver {

    // receives a message
    // message: the message
    void receive(UserCodeMessage message);
}
