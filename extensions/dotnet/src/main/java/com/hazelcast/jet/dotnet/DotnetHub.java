package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

// represents the dotnet hub
public final class DotnetHub {

    // FIXME some of these constants may be advanced config options?

    private final static int DATA_CAPACITY = 1024; // bytes
    private final static int SPIN_DELAY = 4; // milliseconds
    private final static int PROCESS_DEATH_TIMEOUT = 2; // seconds

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

        logger.fine("DotnetHub starting, running " + methodName + " for " + instanceName + " over " + pipeName);

        pipes = new ConcurrentLinkedQueue<>();

        // FIXME better process handling
        //  ensure that the process is still running after startProcess, else throw
        //  if opening pipes then fails, ensure that we stop the process
        //  periodically (?) verify that the process is active, else tear down

        startProcess();
        openPipes();
    }

    // start the dotnet process
    private void startProcess() throws IOException {

        String pipeName = serviceContext.getPipeName();
        String methodName = config.getMethodName();
        String instanceName = serviceContext.getInstanceName();

        // the process accepts three parameters
        // - the pipe name base i.e. <guid>
        // - the number of pipes
        // - the name of the method to execute (?)

        File runtimeDir = serviceContext.getRuntimeDir();
        if (!runtimeDir.exists() || !runtimeDir.isDirectory()) {
            throw new IOException("Invalid runtime directory " + runtimeDir);
        }

        String platform = SystemExtensions.getPlatform();
//        String platformDir = runtimeDir + File.separator + platform;
//        File platformFile = new File(platformDir);
//        if (!platformFile.exists() || !platformFile.isDirectory()) {
//            throw new IOException("Missing platform " + platform);
//        }

        logger.fine("Runtime directory: " + runtimeDir + ", Platform: " + platform);

        String dotnetExe = runtimeDir + File.separator + config.getDotnetExe();
        if (!(new File(dotnetExe).exists())) {
            dotnetExe += ".exe";
        }
        if (!(new File(dotnetExe).exists())) {
            throw new IOException("Could not find executable file " + dotnetExe + ".");
        }

        // on Viridian, we don't have permission to execute the file
        // but we don't have permission to change the permissions either

        // on some OS the file actually needs to be executable
        if (!OsHelper.isWindows()) {
            Path dotnetExePath = Paths.get(dotnetExe);
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(dotnetExePath);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(dotnetExePath, perms);
        }

        int pipesCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        ProcessBuilder builder = new ProcessBuilder(dotnetExe, pipeName, Integer.toString(pipesCount), methodName);

        dotnetProcess = builder
                .directory(runtimeDir)
                .redirectErrorStream(true)
                .start();

        dotnetProcessId = SystemExtensions.processPid(dotnetProcess);
        stdoutLoggingThread = SystemExtensions.logStdOut(dotnetProcess, logger);
        logger.fine("DotnetHub [" + dotnetProcessId + "] started, running " + methodName + " for " + instanceName + " over " + pipeName);
    }

    // stops the dotnet process
    private void stopProcess() {

        logger.fine("DotnetHub `[" + dotnetProcessId + "]` stopping");

        if (!dotnetProcess.isAlive()) {
            logger.fine("DotnetHub `[" + dotnetProcessId + "]` already stopped");
            return;
        }

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
            logger.fine("DotnetHub [" + dotnetProcessId + "] failed to send kiss-of-death to dotnet process", e);
        }

        // keep track of thread interruptions that we are going to catch
        boolean interrupted = false;
        boolean destroyed = false;

        // give the process some time to stop, then kill it for real
        while (true) {
            try {
                if (dotnetProcess.waitFor(PROCESS_DEATH_TIMEOUT, SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.fine("DotnetHub [" + dotnetProcessId + "] ignoring interruption signal in order to prevent process leak");
                interrupted = true;
            }
            logger.fine("DotnetHub [" + dotnetProcessId + "] still running after " + PROCESS_DEATH_TIMEOUT + "s, kill");
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
                logger.fine("DotnetHub [" + dotnetProcessId + "] ignoring interruption signal in order to prevent " + stdoutLoggingThread.getName() + " thread leak");
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

        logger.fine("DotnetHub [" + dotnetProcessId + "] opened " + pipes.size() + " pipes");
    }

    // get a pipe from the hub
    public IJetPipe getPipe() {

        IJetPipe pipe = pipes.poll();
        if (pipe == null) throw new IllegalStateException("Could not provide a pipe.");
        return pipe;
    }

    // wip
    public CompletableFuture<IJetPipe> getPipeX() {

        // TODO: need a way to do this in a nice way + abort/cancel when needed
        IJetPipe pipe;
        while ((pipe = pipes.poll()) == null) {} // uh no!! use a blocking queue of some sort!!
        return CompletableFuture.completedFuture(pipe);
    }

    // return a pipe to the hub
    public void returnPipe(IJetPipe pipe) {

        if (pipe != null) pipes.add(pipe);
    }
}
