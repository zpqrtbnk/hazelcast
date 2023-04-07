package com.hazelcast.jet.dotnet;

import com.hazelcast.logging.ILogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ProcessExtensions {

    private ProcessExtensions() { }

    public static String processPid(Process process) {
        try {
            // Process.pid() is @since 9
            return Process.class.getMethod("pid").invoke(process).toString();
        } catch (Exception e) {
            return process.toString().replaceFirst("^.*pid=(\\d+).*$", "$1");
        }
    }

    public static Thread logStdOut(Process process, ILogger logger) {
        String processId = processPid(process);
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
