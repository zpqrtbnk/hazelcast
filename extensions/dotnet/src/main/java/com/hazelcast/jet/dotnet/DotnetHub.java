package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

// represents the dotnet hub
public final class DotnetHub {

    private final int DATA_CAPACITY = 1024; // bytes
    private final int SPIN_DELAY = 4; // milliseconds
    private final int PROCESS_DEATH_TIMEOUT = 2; // seconds

    private final DotnetServiceContext serviceContext;
    private final DotnetServiceConfig config;
    private final Queue<IJetPipe> pipes;

    private final ILogger logger;
    private Process dotnetProcess;
    private String dotnetProcessId;
    private Thread stdoutLoggingThread;

    // initializes a new dotnet hub
    public DotnetHub(DotnetServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;
        this.config = serviceContext.getConfig();
        this.logger = serviceContext.getLogger();

        String pipeName = serviceContext.getPipeName();
        String methodName = config.getMethodName();
        String instanceName = serviceContext.getInstanceName();

        logger.info("DotnetHub starting, running " + methodName + " for " + instanceName + " over " + pipeName);

        pipes = new ConcurrentLinkedQueue<>();

        startProcess();
        openPipes(); // FIXME is this fails, need to stop the process!
    }

    // start the dotnet process
    private void startProcess() throws IOException {

        String pipeName = serviceContext.getPipeName();
        String methodName = config.getMethodName();
        String instanceName = serviceContext.getInstanceName();

        // the process accepts three parameters
        // - the pipe name base i.e. <guid>
        // - the number of pipes
        // - the name of the method to execute

        String dotnetExe = config.getDotnetPath() + File.separator + config.getDotnetExe();
        int pipesCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        ProcessBuilder builder = new ProcessBuilder(dotnetExe, pipeName, Integer.toString(pipesCount), methodName);

        dotnetProcess = builder
                .directory(Paths.get(config.getDotnetPath()).toFile())
                .redirectErrorStream(true)
                .start();

        dotnetProcessId = ProcessExtensions.processPid(dotnetProcess);
        stdoutLoggingThread = ProcessExtensions.logStdOut(dotnetProcess, logger);
        logger.info("DotnetHub [" + dotnetProcessId + "] started, running " + methodName + " for " + instanceName + " over " + pipeName);
    }

    // stops the dotnet process
    private void stopProcess() {

        logger.info("DotnetHub `[" + dotnetProcessId + "]` stopping");

        // try to send the kiss-of-death to the process, so it stops by itself
        try {
            IJetPipe pipe = pipes.poll();
            if (pipe != null) {
                pipe.write(JetMessage.KissOfDeath);
                logger.fine("DotnetHub [" + dotnetProcessId + "] sent kiss-of-death to dotnet process");
            }
            else {
                logger.fine("DotnetHub [" + dotnetProcessId + "] could not send kiss-of-death to dotnet process");
            }
        }
        catch (Exception e) {
            logger.warning("DotnetHub [" + dotnetProcessId + "] failed to send kiss-of-death to dotnet process", e);
        }

        // keep track of thread interruptions that we are going to catch
        boolean interrupted = false;
        boolean destroyed = false;

        // give the process some time to stop, then kill it
        while (true) {
            try {
                if (dotnetProcess.waitFor(PROCESS_DEATH_TIMEOUT, SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.info("DotnetHub [" + dotnetProcessId + "] ignoring interruption signal in order to prevent process leak");
                interrupted = true;
            }
            logger.warning("DotnetHub [" + dotnetProcessId + "] still running after " + PROCESS_DEATH_TIMEOUT + "s, kill");
            if (destroyed) {
                dotnetProcess.destroyForcibly(); // SIGKILL
            }
            else {
                dotnetProcess.destroy(); // SIGTERM
                destroyed = true;
            }
        }

        // join the logging thread
        while (true) {
            try {
                stdoutLoggingThread.join();
                break;
            } catch (InterruptedException e) {
                logger.info("DotnetHub [" + dotnetProcessId + "] ignoring interruption signal in order to prevent " + stdoutLoggingThread.getName() + " thread leak");
                interrupted = true;
            } catch (Exception e) {
                logger.warning("DotnetHub [" + dotnetProcessId + "] " + stdoutLoggingThread.getName() + " thread has completed with an exception", e);
            }
        }

        // if we have caught an interrupt, interrupt
        if (interrupted) {
            currentThread().interrupt();
        }
    }

    // destroys the dotnet hub
    public void destroy() {

        stopProcess();

        IJetPipe pipe;
        while ((pipe = pipes.poll()) != null) {

            pipe.destroy();
        }
    }

    // open the pipes
    private void openPipes() throws IOException {

        int pipesCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        String pipeName = serviceContext.getPipeName();

        for (int i = 0; i < pipesCount; i++)
            pipes.add(new ShmPipe(false, null, pipeName + "-" + i, DATA_CAPACITY, SPIN_DELAY));

        logger.info("DotnetHub [" + dotnetProcessId + "] opened " + pipes.size() + " pipes");
    }

    // get a pipe from the hub
    public IJetPipe getPipe() {

        // FIXME what-if it's null?
        return pipes.poll();
    }

    // return a pipe to the hub
    public void returnPipe(IJetPipe pipe) {
        if (pipe != null) pipes.add(pipe);
    }
}
