package com.hazelcast.jet.dotnet;

public class ProcessExtensions {

    public static String processPid(Process process) {
        try {
            // Process.pid() is @since 9
            return Process.class.getMethod("pid").invoke(process).toString();
        } catch (Exception e) {
            return process.toString().replaceFirst("^.*pid=(\\d+).*$", "$1");
        }
    }

}
