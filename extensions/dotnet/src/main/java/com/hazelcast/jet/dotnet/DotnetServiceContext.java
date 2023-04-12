package com.hazelcast.jet.dotnet;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import java.util.UUID;

// provides context to the dotnet service
public final class DotnetServiceContext {

    private final ProcessorSupplier.Context processorContext;
    private final DotnetServiceConfig config;

    // initializes the dotnet service context
    DotnetServiceContext(ProcessorSupplier.Context processorContext, DotnetServiceConfig serviceConfig) {

        this.processorContext = processorContext;
        config = serviceConfig;
    }

    // gets the processor context
    public ProcessorSupplier.Context getProcessorContext() {

        return processorContext;
    }

    // gets the dotnet service configuration
    public DotnetServiceConfig getConfig() {

        return config;
    }

    // gets the process logger
    public ILogger getLogger() {

        return processorContext.logger();
    }

    // gets the unique identifier of the pipe
    public String getPipeName() {

        UUID uuid = processorContext.hazelcastInstance().getLocalEndpoint().getUuid();
        return uuid.toString();
    }

    // gets the name of the instance
    public String getInstanceName() {

        return getProcessorContext().hazelcastInstance().getName();
    }
}
