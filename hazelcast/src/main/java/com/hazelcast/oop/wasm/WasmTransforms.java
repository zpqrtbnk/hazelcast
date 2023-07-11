package com.hazelcast.oop.wasm;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.concurrent.CompletableFuture;

public class WasmTransforms {

    // maps using wasm
    @Nonnull
    public static <TInput, TResult> FunctionEx<StreamStage<TInput>, StreamStage<TResult>> mapAsync(
            @Nonnull BiFunctionEx<WasmService, ? super TInput, ? extends CompletableFuture<TResult>> mapAsyncFn) {

        final int maxConcurrentOps = 1;
        final boolean preserveOrder = true;

        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."

        ServiceFactory<?, WasmService> serviceFactory = ServiceFactories
                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                .sharedService(
                        processorContext -> new WasmService(new WasmServiceContext(processorContext)),
                        WasmService::destroy);

        // FIXME really, what's 'directory' here?!
        serviceFactory.withAttachedDirectory("wasm-", new File(""));

        return s -> s
                .mapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder, mapAsyncFn)
                .setName("wasm-transform");
    }
}
