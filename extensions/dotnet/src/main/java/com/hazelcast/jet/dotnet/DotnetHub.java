package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.ILogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class DotnetHub {

    private final DotnetServiceContext serviceContext;
    private final Queue<AsynchronousFileChannel> channels;
    private final ILogger logger;
    private Process dotnetProcess;
    private String dotnetProcessId;
    private Thread stdoutLoggingThread;

    DotnetHub (DotnetServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        this.channels = new ConcurrentLinkedQueue<>();
        this.logger = serviceContext.getLogger();

        String instanceName = serviceContext.getInstanceName();
        serviceContext.getLogger().info("Created dotnet hub for " + instanceName);
        try {
            startProcess();
            openChannels();
        }
        catch (Exception e) {
            serviceContext.getLogger().severe(e);
            // FIXME and then what?
        }
    }

    public void destroy() {
        stopProcess();
    }

    public AsynchronousFileChannel getChannel() {
        AsynchronousFileChannel channel = channels.poll();
        serviceContext.getLogger().fine("Get channel... " + (channel == null ? "null!" : ""));
        return channel; // FIXME what-if it's null?
    }

    public void returnChannel(AsynchronousFileChannel channel) {
        serviceContext.getLogger().fine("Return channel...");
        if (channel != null) channels.add(channel);
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

    private void openChannels() throws IOException {

        // TODO: we need to wait for the process to be ready = retry

        DotnetServiceConfig config = serviceContext.getConfig();
        int channelsCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        Path pipePath = serviceContext.getPipePath();

        // hold on, openAsynchronousChannel already has a retry mechanism, so what?!
        //boolean success = false;
        //AsynchronousFileChannel channel;
        //try {
        //    channel = openAsynchronousChannel(pipePath);
        //}

        for (int i = 0; i < channelsCount; i++)
            channels.add(openAsynchronousChannel(pipePath)); // FIXME what-if it fails?

        logger.info("Opened " + channels.size() + " channels to hub for " + serviceContext.getInstanceName() + " at " + serviceContext.getPipeName());
    }

    private void stopProcess() {

        // TODO: monitor that the process exits, else force-kill it
        String instanceName = serviceContext.getInstanceName();
        logger.info("Stopping dotnet hub [" + dotnetProcessId + "] for " + instanceName + " at " + serviceContext.getPipeName());
        try {
            AsynchronousFileChannel channel = channels.poll();
            if (channel != null) {
                ChannelExtensions.writeInteger(channel, -1);
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

    private AsynchronousFileChannel openAsynchronousChannel(Path pipePath) throws IOException {
        AsynchronousFileChannel channel = null;
        int retries = 0; // TODO: timeout and number of retries should be options
        while (channel == null && retries++ < 10) {
            try {
                channel = AsynchronousFileChannel.open(pipePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
            catch (FileSystemException fse) {
                if (retries == 10) {
                    fse.printStackTrace();
                    throw fse; // rethrow
                }
                // else retry
                channel = null;
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                }
                catch (InterruptedException ie) {
                    // ??
                }
            }
        }
        return channel;
    }
}
