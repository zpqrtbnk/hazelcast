package com.hazelcast.jet.python;

import java.io.IOException;

public class PythonHub {

    // similar to the .NET DotnetHub class, this should create and manage the Python process
    // then
    // XxxConfig should be kept minimal (do we event need it?)
    // XxxContext should be kept minimal, just restoring the resources, etc
    // XxxService creates the XxxHub and then uses it to communicate with the external process
    // service hubs could have different implementations (long-running, containerized...)

    // on the .NET side:
    // DeserializingEntryExtensions would be a common ext thing
    // most of the SharedMemory, IJetPipe, JetMessage etc should be common things
    // DotnetSubmit should be ... removed
    //
    // Example, Transforms... are for test purposes and should be removed

    // are there things we could factor in a ServiceHub base class?
    // the constructor should *not* start the hub,
    // we should have an explicit start() method + destroy() to release

    private final PythonServiceContext serviceContext;

    public PythonHub(PythonServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;
    }

    public void start() {

        // we need to abstract the run of a process to avoid duplicating code all over the place
    }

    private void meh(String... command) {

//         ProcessBuilder processBuilder = new ProcessBuilder(command);
//         Process process = processBuilder
//                 .directory("")
//                 .redirectErrorStream(true) // what about output stream?
//                 .start();
         //String processPid = SystemExtensions.getProcessPid(process);
         //Thread loggingThread = SystemExtensions.logStdOut(process);
    }

    public void destroy() {

    }
}
