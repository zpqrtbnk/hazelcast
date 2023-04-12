package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DotnetHub2 {

    private final DotnetServiceContext serviceContext;
    //private final IJetPipe pipe;
    private final Queue<IJetPipe> pipes;

    private final ILogger logger;
    private Process dotnetProcess;
    private String dotnetProcessId;
    private Thread stdoutLoggingThread;

    public DotnetHub2(DotnetServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        this.logger = serviceContext.getLogger();

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("Created dotnet hub for " + instanceName);

        pipes = new ConcurrentLinkedQueue<>();

        try{
            startProcess();
            openPipes();
        }
        catch (Exception e) {
            serviceContext.getLogger().severe(e);
            // FIXME and then what?
            return;
        }

        /*
        IJetPipe p = null;
        try {
            startProcess();
            p = new ShmPipe2(false, null, serviceContext.getPipeName(), 1024, 500);
        }
        catch (Exception e) {
            serviceContext.getLogger().severe(e);
            // FIXME and then what?
        }
        pipe = p;
        */
    }

    private void openPipes() throws IOException {

        DotnetServiceConfig config = serviceContext.getConfig();
        int pipesCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        String pipeName = serviceContext.getPipeName();

        for (int i = 0; i < pipesCount; i++)
            pipes.add(new ShmPipe2(false, null, pipeName + "-" + i, 1024, 4));

        logger.info("Opened " + pipes.size() + " pipes to hub for " + serviceContext.getInstanceName() + " at " + serviceContext.getPipeName());
    }

    private void startProcess() throws IOException {
        DotnetServiceConfig config = serviceContext.getConfig();
        String pipeName = serviceContext.getPipeName();
        String methodName = config.getMethodName();

        String dotnetExe = config.getDotnetPath() + File.separator + config.getDotnetExe();
        int channelsCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        ProcessBuilder builder = new ProcessBuilder(dotnetExe, pipeName, Integer.toString(channelsCount), methodName);
        dotnetProcess = builder
                .directory(Paths.get(config.getDotnetPath()).toFile())
                .redirectErrorStream(true)
                .start();

        dotnetProcessId = ProcessExtensions.processPid(dotnetProcess);
        stdoutLoggingThread = ProcessExtensions.logStdOut(dotnetProcess, logger);
        logger.info("Started dotnet hub [" + dotnetProcessId + ":" + methodName + "] for " + serviceContext.getInstanceName() + " at " + pipeName);
    }

    private void stopProcess() {

        // TODO: monitor that the process exits, else force-kill it
        String instanceName = serviceContext.getInstanceName();
        logger.info("Stopping dotnet hub [" + dotnetProcessId + "] for " + instanceName + " at " + serviceContext.getPipeName());
        try {
            IJetPipe pipe = pipes.poll();
            if (pipe != null) {
                pipe.write(JetMessage.KissOfDeath);
                logger.fine("Hub sent signal of death to dotnet process");
            }
            else {
                logger.fine("Could not send signal of death to dotnet process");
            }
        }
        catch (Exception e) {
            logger.warning(e);
        }

        // TODO: see python code, here we expect the process to terminate nicely
        // but if we cannot get a channel or for other reasons, we might have to kill it?

        try { // TODO: do better
            stdoutLoggingThread.join();
        }
        catch (Exception e) { }
    }

    public void destroy() {
        stopProcess();
    }

    public IJetPipe getPipe() {
        //return pipe;
        IJetPipe pipe = pipes.poll();
        serviceContext.getLogger().fine("Get pipe... " + (pipe == null ? "null!" : ""));
        return pipe; // FIXME what-if it's null?
    }

    public void returnPipe(IJetPipe pipe) {
        serviceContext.getLogger().fine("Return pipe...");
        if (pipe != null) pipes.add(pipe);
    }
}
