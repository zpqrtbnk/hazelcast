package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;

public class ProcessJava {

    public static String process(int input, DotnetServiceContext context) {
        return "__" + input + "__";
    }

    public static CompletableFuture<String> processAsync(int input, DotnetServiceContext context) {
        return CompletableFuture.completedFuture(process(input, context));
    }
}
