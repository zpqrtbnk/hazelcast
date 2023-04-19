package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.logging.ILogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;

// provides system methods
final class SystemExtensions {

    private SystemExtensions() { }

    // gets the identifier of a process
    public static String processPid(Process process) {

        try {
            // Process.pid() is available with Java 9
            return Process.class.getMethod("pid").invoke(process).toString();
        } catch (Exception e) {
            return process.toString().replaceFirst("^.*pid=(\\d+).*$", "$1");
        }
    }

    // starts and returns a thread that copies the standard output of a process to a logger
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

    // gets the current platform
    public static String getPlatform() {
        String platform =
                OsHelper.isWindows() ? "win-x64" :
                OsHelper.isLinux() ? "linux-x64" :
                OsHelper.isMac() ? "osx-x64" :
                //OsHelper.isUnixFamily() ? "unknown" :
                "unknown";

        return platform;
    }
}
