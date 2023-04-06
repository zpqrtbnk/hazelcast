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

public class DotnetHub {

    private static Map<String, DotnetHub> hubs = new HashMap<>();
    private int references;
    private DotnetServiceContext serviceContext;
    private Process dotnetProcess;
    private String dotnetProcessId;
    private Queue<AsynchronousFileChannel> channels;
    private ILogger logger;
    private Thread stdoutLoggingThread;

    DotnetHub (DotnetServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        this.channels = new ConcurrentLinkedQueue<>();
        this.logger = serviceContext.getLogger();
    }

    public static DotnetHub get(DotnetServiceContext serviceContext) {
        // each instance (member) has its own hub which controls 1 dotnet process
        // TODO: in real-life ... we may want to merge hub and service
        String instanceName = serviceContext.getInstanceName();
        DotnetHub hub;
        if (hubs.containsKey(instanceName)) {
            hub = hubs.get(instanceName);
            hub.references++;
            return hub;
        }

        hub = new DotnetHub(serviceContext);
        hub.references++;
        serviceContext.getLogger().info("Created dotnet hub for " + instanceName);
        try {
            hub.startProcess();
            hub.connectChannels();
        }
        catch (Exception e) {
            serviceContext.getLogger().severe(e);
        }
        hubs.put(instanceName, hub);
        return hub;
    }

    public void free() {
         if (--references == 0) destroy();
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

        String dotnetExe = config.getDotnetPath() + File.separator + config.getDotnetExe();
        int channelsCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        ProcessBuilder builder = new ProcessBuilder(dotnetExe, pipeName, Integer.toString(channelsCount));
        dotnetProcess = builder
                .directory(Paths.get(config.getDotnetPath()).toFile())
                .redirectErrorStream(true)
                .start();

        dotnetProcessId = ProcessExtensions.processPid(dotnetProcess);
        stdoutLoggingThread = logStdOut(logger, dotnetProcess, dotnetProcessId);
        logger.info("Started dotnet hub [" + dotnetProcessId + "] for " + serviceContext.getInstanceName() + " at " + pipeName);
    }

    private void connectChannels() throws IOException {
        DotnetServiceConfig config = serviceContext.getConfig();
        int channelsCount = config.getLocalParallelism() * config.getMaxConcurrentOps();
        Path pipePath = serviceContext.getPipePath();
        for (int i = 0; i < channelsCount; i++)
            channels.add(openAsynchronousChannel(pipePath)); // FIXME what-if it fails?

        logger.info("Connected " + channels.size() + " dotnet hub channels for " + serviceContext.getInstanceName() + " at " + serviceContext.getPipeName());
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
                }
                channel = null;
                try {
                    TimeUnit.MILLISECONDS.sleep(200);
                }
                catch (InterruptedException ie) {
                    // ??
                }
            }
        }
        return channel;
    }

    static Thread logStdOut(ILogger logger, Process process, String processId) {
        Thread thread = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
                for (String line; (line = in.readLine()) != null; ) {
                    logger.fine(line);
                }
            } catch (IOException e) {
                logger.severe("Reading init script output failed", e);
            }
        }, "dotnet-hub-logger-" + processId);
        thread.start();
        return thread;
    }
}
