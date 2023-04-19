package com.hazelcast.jet.dotnet;

import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

// provides context to the dotnet service
public final class DotnetServiceContext {

    private final ProcessorSupplier.Context processorContext;
    private final DotnetServiceConfig config;
    private final ILogger logger;
    private final File runtimeDir;

    // initializes the dotnet service context
    DotnetServiceContext(ProcessorSupplier.Context processorContext, DotnetServiceConfig serviceConfig) {

        this.processorContext = processorContext;
        logger = getLogger(getClass().getPackage().getName());
        config = serviceConfig;

        // recreate the dotnet directory (only for the current platform)
        // this code runs on each member where the job is running
        String platform = SystemExtensions.getPlatform();

        // writes
        // RESOURCES:
        //   hazelcast-jet-dotnet-5.3.0-SNAPSHOT.jar
        //     from: .addJar("hazelcast/extensions/dotnet/target/hazelcast-jet-dotnet-5.3.0-SNAPSHOT.jar")
        //   dotnet-jet-1.0-SNAPSHOT.jar
        //   org/example/DotnetJet.class
        //     from: .addClass(DotnetJet.class)
        //   dotnet-eba0eb5e- <<< WTF is this?! where is linux-x64?!
        //     from: jobConfig.attachDirectory(directory, id);
        //       id is "dotnet-" + platform
        //       platform is ??
        Map<String, ResourceConfig> resources = processorContext.jobConfig().getResourceConfigs();
        String s = "RESOURCES: ";
        for (String e : resources.keySet()) s += " " + e;
        logger.info(s);

        runtimeDir = processorContext.recreateAttachedDirectory(config.getDotnetDirId(platform));
    }

    // gets the processor context
    public ProcessorSupplier.Context getProcessorContext() {

        return processorContext;
    }

    // gets the dotnet service configuration
    public DotnetServiceConfig getConfig() {

        return config;
    }

    // gets the service logger
    public ILogger getLogger() {

        return logger;
    }

    // gets a logger
    public ILogger getLogger(String name) {

        return processorContext.hazelcastInstance().getLoggingService().getLogger(name);
    }

    // gets the runtime directory
    public File getRuntimeDir() {

        return runtimeDir;
    }

    // gets the unique identifier of the pipe
    public String getPipeName() {

        UUID uuid = processorContext.hazelcastInstance().getLocalEndpoint().getUuid();
        return uuid.toString();
    }

    // gets the name of the instance
    public String getInstanceName() {

        return processorContext.hazelcastInstance().getName();
    }
}
