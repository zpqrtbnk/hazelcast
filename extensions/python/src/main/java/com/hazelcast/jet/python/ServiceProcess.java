package com.hazelcast.jet.python;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.io.*;
import java.util.Optional;

import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ServiceProcess {

    private final static int PROCESS_DEATH_TIMEOUT = 2; // seconds

    private String name;
    private File directory;
    private String[] command;
    private ILogger logger;
    private Process process;
    private Thread logging;

    public ServiceProcess(String name, LoggingService loggingService) {
        this.name = name;
        this.logger = loggingService.getLogger(getClass() + ":" + name);
    }

    public ServiceProcess(String name, File directory, LoggingService loggingService) {
        this(name, loggingService);
        this.directory = directory;
    }

    public ServiceProcess command(String... command) {
        this.command = command;
        return this;
    }

    // gets the name of the process
    public String name() { return name; }

    // gets the process system identifier
    public long pid() { return process.pid(); }

    public ServiceProcess start() throws IOException {

        if (process != null) {
            throw new IllegalStateException("Process was already started.");
        }

        if (directory == null) {
            throw new IllegalStateException("Directory was not initialized.");
        }

        if (command == null || command.length == 0) {
            throw new IllegalStateException("Command was not initialized.");
        }

        ProcessBuilder processBuilder = new ProcessBuilder(command);

        process = processBuilder
                .directory(directory)
                .redirectErrorStream(true) // stderr > stdout
                .start();

        logging = logStdOut();

        if (isAlive()) {
            logger.info("Process " + name + "-" + process.pid() + " has started.");
            return this;
        }

        logger.info("Process " + name + "-" + process.pid() + " has stopped immediately.");
        process = null;
        stopLogging(true);

        throw new IOException("Process failed to start.");
    }

    // writes to the process stdin
    public void write(String s) {
        try {
            new PrintStream(process.getOutputStream(), true, UTF_8.name()).println(s);
        } catch (UnsupportedEncodingException e) {
            logger.info("UTF_8 reported as unsupported encoding??");
        }
    }

    // stops the process
    // assumes that the caller has already instructed the process to stop in a nice way
    public void stop() {

        if (process == null) { return; }

        // give the process some time to stop, then kill it for real
        boolean interrupted = false;
        boolean destroyed = false;
        while (true) {
            try {
                if (process.waitFor(PROCESS_DEATH_TIMEOUT, SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.info("Ignoring interruption signal in order to prevent process leak");
                interrupted = true;
            }
            if (destroyed) {
                logger.warning("Process " + name + "-" + process.pid() + " still running, kill");
                process.destroyForcibly(); // SIGKILL
            }
            else {
                logger.warning("Process " + name + "-" + process.pid() + " still running, terminate");
                process.destroy(); // SIGTERM
                destroyed = true;
            }
        }

        interrupted |= stopLogging(false);

        logger.info("Process " + name + "-" + process.pid() + " has stopped.");

        process = null;

        if (interrupted) {
            currentThread().interrupt();
        }
    }

    private boolean stopLogging(boolean interrupt)
    {
        boolean interrupted = false;

        while (true) {
            try {
                logging.join();
                break;
            } catch (InterruptedException e) {
                logger.info("Ignoring interruption signal in order to prevent logging thread leak.");
                interrupted = true;
            }
        }

        logging = null;

        if (interrupted && interrupt) {
            currentThread().interrupt();
        }

        return interrupted;
    }

    public boolean isAlive() {
        if (process == null) { return false; }
        Optional<ProcessHandle> optionalHandle = ProcessHandle.of(process.pid());
        if (optionalHandle.isEmpty()) { return false; }
        ProcessHandle handle = optionalHandle.get();
        return handle.isAlive();
    }

    public void destroy() {
        stop();
    }

    // starts and returns a thread that copies the standard output of a process to a logger
    private Thread logStdOut() {

        Thread thread = new Thread(() -> {

            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
                for (String line; (line = in.readLine()) != null; ) {
                    logger.fine(line);
                }
            } catch (IOException e) {
                logger.severe("Exception while logging process " + name + "-" + process.pid() + " stdout.", e);
            }
        }, name + "-" + process.pid());

        thread.start();
        return thread;
    }
}
