package com.hazelcast.jet.ext.client;

import com.hazelcast.jet.impl.operation.AsyncJobOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class JetDebugOperation extends AsyncJobOperation {

    private String request;

    public JetDebugOperation() {

    }

    public JetDebugOperation(String request) {

        this.request = request;
    }

    @Override
    protected CompletableFuture<?> doRun() throws Exception {

        // well, even this will fail on Viridian because, access denied ("java.lang.RuntimePermission" "getenv.PATH")
        // so, I guess we can't even thing about forking anything interesting like Python or .NET
        // regardless of whether we upload them or... have them pre-installed on the machine
        // it's a Java thing?
        //  at java.security.AccessControlContext.checkPermission(...) in AccessControlContext.java:472
        //  at java.security.AccessController.checkPermission(...) in AccessController.java:897
        //  at java.lang.SecurityManager.checkPermission(...) in SecurityManager.java:322
        //  at java.lang.System.getenv(...) in System.java:999

        String path = System.getenv("PATH");
        return CompletableFuture.completedFuture("OK (\n" +
                "  request: " + request + "\n" +
                "  path: " + path + "\n" +
                ")");
    }

    @Override
    public int getClassId() {
        return JetDebugDataSerializerHook.DEBUG;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {

        super.writeInternal(out);
        out.writeString(request);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {

        super.readInternal(in);
        request = in.readString();
    }
}
