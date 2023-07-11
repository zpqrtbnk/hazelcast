package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.oop.channel.IJetPipe;
import com.hazelcast.oop.JetMessage;
import com.hazelcast.oop.SharedMemoryPipe;
import com.hazelcast.oop.service.ServiceProcess;
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

// represents the dotnet hub
public final class DotnetHub {

    // FIXME some of these constants may be advanced config options?

    private final static int DATA_CAPACITY = 1024; // bytes
    private final static int SPIN_DELAY = 4; // milliseconds

    private final DotnetServiceContext serviceContext;
    private final DotnetServiceConfig config;
    private final Queue<IJetPipe> pipes;

    private final ILogger logger;
    private ServiceProcess dotnetProcess;

    // initializes a new dotnet hub
    public DotnetHub(DotnetServiceContext serviceContext) throws IOException {

        this.serviceContext = serviceContext;
        this.config = serviceContext.getConfig();
        this.logger = serviceContext.getLogger();

        String pipeName = serviceContext.getPipeName();
        String instanceName = serviceContext.getInstanceName();

        logger.fine("DotnetHub starting, running for " + instanceName + " over " + pipeName);

        pipes = new ConcurrentLinkedQueue<>();

        // FIXME better process handling
        //  ensure that the process is still running after startProcess, else throw
        //  if opening pipes then fails, ensure that we stop the process
        //  periodically (?) verify that the process is active, else tear down

        startProcess();
        // FIXME we really should wait for the process to notify that it's OK or have a way to fail
        openPipes();
    }

    // start the dotnet process
    private void startProcess() throws IOException {

        String pipeName = serviceContext.getPipeName();
        String instanceName = serviceContext.getInstanceName();

        // the process accepts three parameters
        // - the pipe name base i.e. <guid>
        // - the number of pipes
        // - the name of the method to execute (?)

        File runtimeDir = serviceContext.getRuntimeDir();
        if (!runtimeDir.exists() || !runtimeDir.isDirectory()) {
            throw new IOException("Invalid runtime directory " + runtimeDir);
        }

        String platform = ServiceProcess.getPlatform();
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

        dotnetProcess = new ServiceProcess("dotnet-jet", runtimeDir, serviceContext.getLoggingService())
                .command(dotnetExe, pipeName, Integer.toString(pipesCount))
                .start();

        logger.fine("DotnetHub [" + dotnetProcess.pid() + "] started, running for " + instanceName + " over " + pipeName);
    }

    // stops the dotnet process
    private void stopProcess() {

        logger.fine("DotnetHub `[" + dotnetProcess.pid() + "]` stopping");

        if (!dotnetProcess.isAlive()) {
            logger.fine("DotnetHub `[" + dotnetProcess.pid() + "]` already stopped");
            return;
        }

        // try to send the kiss-of-death to the process, so it stops by itself
        try {
            IJetPipe pipe = pipes.poll();
            if (pipe != null) {
                pipe.write(JetMessage.KissOfDeath);
                logger.fine("DotnetHub [" + dotnetProcess.pid() + "] sent kiss-of-death to dotnet process");
            }
            else {
                logger.fine("DotnetHub [" + dotnetProcess.pid() + "] could not send kiss-of-death to dotnet process");
                }
            }
        catch (Exception e) {
            logger.fine("DotnetHub [" + dotnetProcess.pid() + "] failed to send kiss-of-death to dotnet process", e);
        }

        dotnetProcess.stop();
    }

    // destroys the dotnet hub
    public void destroy() {

        stopProcess();

        for (IJetPipe pipe; (pipe = pipes.poll()) != null; ) pipe.destroy();
    }

    // open the pipes
    private void openPipes() throws IOException {

        int pipesCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        String pipeName = serviceContext.getPipeName();

        for (int i = 0; i < pipesCount; i++)
            pipes.add(new SharedMemoryPipe(false, null, pipeName + "-" + i, DATA_CAPACITY, SPIN_DELAY));

        logger.fine("DotnetHub [" + dotnetProcess.pid() + "] opened " + pipes.size() + " pipes");
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
