package com.hazelcast.usercode;

import java.util.concurrent.Future;

// mapUsingXxx
//  var runtime = member.getUserCodeService().start(...);
//  var args = new byte[][];
//  var response = runtime.invoke("method", args);
//  member.getUserCodeService().destroy(runtime);

// represents a UserCode service
// - starts a UserCode runtime
// - stops a UserCode runtime
// - invokes a UserCode runtime
public interface UserCodeService {

    // starts a runtime
    // name: the unique name of the runtime
    // startInfo: arguments for the runtime creation
    // returns: the runtime
    Future<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeStartInfo startInfo) throws UserCodeException;

    // destroys a runtime
    // runtime: the runtime
    Future<Void> destroyRuntime(UserCodeRuntime runtime);
}


