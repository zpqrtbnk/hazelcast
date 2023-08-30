package com.hazelcast.jet.usercode;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.jobbuilder.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.*;

import java.util.concurrent.ExecutionException;

public class UserCodeStageProvider implements JobBuilderStageProvider {

    @Override
    public SourceStage[] getSources() {
        return null;
    }

    @Override
    public TransformStage[] getTransforms() {

        return new TransformStage[] {
                new TransformStage("user-code", UserCodeStageProvider::transform)
        };
    }

    @Override
    public SinkStage[] getSinks() {
        return null;
    }

    private static Object transform(Object stageContext, String name, JobBuilderInfoMap definition, ILogger logger) throws JobBuilderException {

        UserCodeRuntimeInfo runtimeInfo = new UserCodeRuntimeInfo(definition.childAsMap("runtime"));

        String functionName = definition.childAsString("function");
        String transformName = definition.childAsString("name", "user-code");
        Boolean preserveOrder = definition.childAsBoolean("preserve-order", true);

        int parallelProcessors = definition.childAsInteger("parallel-processors", 1);
        int parallelOperations = definition.childAsInteger("parallel-operations", 1);

        StreamStage streamStage = stageContext instanceof StreamStage ? (StreamStage) stageContext : null;
        if (streamStage == null) {
            throw new JobBuilderException("panic: unsupported stage type " + stageContext.toString());
        }

        // "When you submit a job, Jet serializes ServiceFactory and sends it to all the cluster members."
        //
        // "On each member Jet calls createContextFn() to get a context object that will be shared across
        // all the service instances on that member. For example, if you are connecting to an external
        // service that provides a thread-safe client, you can create it here and then create individual
        // sessions for each service instance."
        //
        // "Jet repeatedly calls {@link #createServiceFn()} to create as many service instances on each
        // member as determined by the localParallelism of the pipeline stage. The invocations of
        // createServiceFn() receive the context object.
        //
        // "When the job is done, Jet calls destroyServiceFn() with each service instance. Finally, Jet
        // calls destroyContextFn() with the context object."

        ServiceFactory<?, UserCodeRuntime> serviceFactory = ServiceFactories

                // shared: "the service is thread-safe and can be called from multiple-threads, so Hazelcast
                // will create just one instance on each member and share it among the parallel task-lets."
                //
                // here, createContextFn (which creates the thing that is shared) creates the service
                // itself, and createServiceFn (which creates each service instance) just passes the
                // service along.

                .sharedService(
                        x -> startUserCodeRuntime(x, transformName, runtimeInfo),
                        UserCodeStageProvider::destroyUserCodeRuntime);

        return streamStage
                .mapUsingServiceAsync(serviceFactory, parallelOperations, preserveOrder, (service, input) -> {

                    UserCodeRuntime runtime = (UserCodeRuntime) service;
                    return runtime.invoke(functionName, input);
                })
                .setLocalParallelism(parallelProcessors)
                .setName(transformName);
    }

    private static UserCodeRuntime startUserCodeRuntime(ProcessorSupplier.Context processorContext, String name, UserCodeRuntimeInfo runtimeInfo) {

        // TODO: implement this - should be some sort of static user code service?!
        //UserCodeService userCodeService = processorContext.hazelcastInstance().getUserCodeService();
        LoggingService logging = processorContext.hazelcastInstance().getLoggingService();
        logging.getLogger(UserCodeStageProvider.class).info("start user code runtime");
        String serviceName;
        if (runtimeInfo.childIsString("service")) {
            serviceName = runtimeInfo.childAsString("service");
        }
        else {
            JobBuilderInfoMap serviceInfo = runtimeInfo.childAsMap("service");
            serviceName = serviceInfo.uniqueChildName();
        }
        HazelcastInstance hazelcastInstance = processorContext.hazelcastInstance();
        UserCodeService userCodeService = UserCodeServiceFactory.getService(hazelcastInstance, serviceName, logging);
        processorContext.managedContext().initialize(userCodeService); // will need the serialization service

        // will need it for resources
        runtimeInfo.setProcessorContext(processorContext);

        // complement name, name does not have to be unique, user code service will manage it
        long jobId = processorContext.jobId();
        name = jobId + "-" + name;

        try {
            // TODO: can we copy files and resources over to the other side?
            UserCodeRuntime runtime = userCodeService.startRuntime(name, runtimeInfo).get();
            //runtime.copyTo(localFilePath, targetFilePath).get();
            //runtime.invoke("HZ-COPY-TO", ...)
            return runtime;
        }
        catch (InterruptedException | ExecutionException ex) {
            throw new UserCodeException("Failed to start runtime.", ex);
        }
    }

    private static void destroyUserCodeRuntime(UserCodeRuntime runtime) {

        try {
            runtime.getUserCodeService().destroyRuntime(runtime).get();
        }
        catch (InterruptedException | ExecutionException ex) {
            throw new UserCodeException("Failed to destroy runtime.", ex);
        }
    }
}
