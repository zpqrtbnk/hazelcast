package com.hazelcast.jet.ext.process;

//import com.hazelcast.internal.util.OsHelper;
//import com.hazelcast.logging.ILogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ServiceProcess {

    private Process process;
    private long pid;

    private void start(String... command) {

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        //process = processBuilder
        //        .directory("")
        //        .redirectErrorStream(true) // what about output stream?
        //        .start();
        //String processPid = SystemExtensions.getProcessPid(process);
        //Thread loggingThread = SystemExtensions.logStdOut(process);
    }

    public void destroy() {

    }

    //public long pid() { return process.pid(); }

    public static String processPid(Process process) {

        try {
            // Process.pid() is available with Java 9
            return Process.class.getMethod("pid").invoke(process).toString();
        } catch (Exception e) {
            return process.toString().replaceFirst("^.*pid=(\\d+).*$", "$1");
        }
    }

    // starts and returns a thread that copies the standard output of a process to a logger
//    public static Thread logStdOut(Process process, ILogger logger) {
//
//        String processId = processPid(process);
//        Thread thread = new Thread(() -> {
//
//            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
//                for (String line; (line = in.readLine()) != null; ) {
//                    logger.fine(line);
//                }
//            } catch (IOException e) {
//                logger.severe("Reading init script output failed", e);
//            }
//        }, "dotnet-hub-logger-" + processId);
//        thread.start();
//        return thread;
//    }

    // gets the current platform e.g. linux-x64
//    public static String getPlatform() {
//
//        // note: missing other OS (solaris...) here
//        String os =
//                OsHelper.isWindows() ? "win" :
//                OsHelper.isLinux() ? "linux" :
//                OsHelper.isMac() ? "osx" :
//                "any";
//
//        // note: missing other architectures (aarch64, ppc...) here
//        String arch = System.getProperty("os.arch");
//        if (arch.equals("amd64")) arch = "x64";
//        if (arch.equals("x86_32")) arch = "x86";
//        if (arch.equals("x86_64")) arch = "x64";
//
//        return os + "-" + arch;
//    }
}
