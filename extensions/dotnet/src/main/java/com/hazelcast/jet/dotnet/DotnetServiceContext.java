package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

class DotnetServiceContext {

    private final ProcessorSupplier.Context processorContext;
    private final DotnetServiceConfig config;

    DotnetServiceContext(ProcessorSupplier.Context processorContext, DotnetServiceConfig serviceConfig) {
        this.processorContext = processorContext;
        config = serviceConfig;
    }

    public ProcessorSupplier.Context getProcessorContext() { return processorContext; }

    public DotnetServiceConfig getConfig() { return config; }

    public ILogger getLogger() { return processorContext.logger(); }

    public String getPipeName() {
        UUID uuid = processorContext.hazelcastInstance().getLocalEndpoint().getUuid();
        return config.getPipeName() + "-" + uuid;
    }

    public Path getPipePath() {
        return Paths.get((OsHelper.isWindows()
                ? "\\\\.\\pipe\\"
                : "/tmp/CoreFXPipe_") + getPipeName());
    }

    public String getInstanceName() { return getProcessorContext().hazelcastInstance().getName(); }
}
