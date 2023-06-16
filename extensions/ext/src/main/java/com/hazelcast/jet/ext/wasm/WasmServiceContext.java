package com.hazelcast.jet.ext.wasm;

import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class WasmServiceContext {

    private final ProcessorSupplier.Context processorContext;
    private final ILogger logger;
    private final File wasmDir;

    // initializes the wasm service context
    WasmServiceContext(ProcessorSupplier.Context processorContext) {

        this.processorContext = processorContext;
        logger = getLogger(getClass().getPackage().getName());
//
//        Map<String, ResourceConfig> resources = processorContext.jobConfig().getResourceConfigs();
//        String s = "RESOURCES: ";
//        for (String e : resources.keySet()) s += " " + e;
//        logger.info(s);
//
//        String platform = ""; // FIXME
//        String directoryId = config.getDotnetDirId(platform);
//        logger.info("Restore directory " + directoryId);
        String directoryId = "";
        wasmDir = processorContext.recreateAttachedDirectory(directoryId);
    }

    // gets the processor context
    public ProcessorSupplier.Context getProcessorContext() {

        return processorContext;
    }

    // gets the service logger
    public ILogger getLogger() {

        return logger;
    }

    // gets a logger
    public ILogger getLogger(String name) {

        return processorContext.hazelcastInstance().getLoggingService().getLogger(name);
    }

    // gets the wasm directory
    public File getWasmDir() {

        return wasmDir;
    }
    // gets the name of the instance
    public String getInstanceName() {

        return processorContext.hazelcastInstance().getName();
    }
}
