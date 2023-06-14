package com.hazelcast.jet.ext.wasm;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class WasmService {

    // FIXME
    // we're missing a nice way to run wasm within the Java process
    // (assuming we want to do it)
    // Wasmer's bridge development is stale, Wasmtime is ... not complete
    // Then there's graal/wasm (https://github.com/oracle/graal/tree/master/wasm)
    // that *may* look promising ...

    private final WasmServiceContext serviceContext;

    //private Instance instance;

    // initializes a new dotnet service
    WasmService(WasmServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;

        Path wasmPath = serviceContext.getWasmDir().toPath();
        byte[] wasmBytes = Files.readAllBytes(wasmPath);
        //instance = new Instance(wasmBytes);

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().fine("WasmService created for " + instanceName);
    }

    // maps using dotnet
    public CompletableFuture<Data[]> mapAsync(Data[] input) {
        // what kind of function should we implement?
        // should we pass a JetMessage to WASM?
        // what kind of arguments are supported?
        //Object[] results = instance.exports.getFunction("sum").apply(2,3);
        //int result = (Integer) results[0];
        return null;
    }

    public void destroy() {

        //instance.close();
    }
}
