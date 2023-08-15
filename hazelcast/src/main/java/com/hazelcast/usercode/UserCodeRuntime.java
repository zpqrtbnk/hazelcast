package com.hazelcast.usercode;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.util.concurrent.CompletableFuture;

// represents a UserCode runtime
public interface UserCodeRuntime  {

    // gets the SerializationService
    SerializationService getSerializationService();

    // gets the UserCodeService that created this runtime
    UserCodeService getUserCodeService();

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request object
    // returns: the response object
    CompletableFuture<?> invoke(String functionName, Object payload);

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request data
    // returns: the response data
    CompletableFuture<Data> invoke(String functionName, Data payload);

    // invokes a runtime function
    // functionName: the name of the function
    // payload: the request bytes
    // returns: the response bytes
    CompletableFuture<byte[]> invoke(String functionName, byte[] payload);
}
