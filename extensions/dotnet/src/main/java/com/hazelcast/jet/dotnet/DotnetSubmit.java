package com.hazelcast.jet.dotnet;

import java.io.File;

public final class DotnetSubmit {

    public static DotnetServiceConfig getConfig(String[] args) {

        String argDirectory = null;
        String argJobName = "dotjet";
        String argDotnetExe = null;

        int argi = 0;
        while (argi < args.length && args[argi].startsWith("-")) {
            if (args[argi].equals("-d")) {
                if (++argi < args.length) {
                    argDirectory = args[argi++];
                }
                else {
                    argi = -1;
                    break;
                }
            }
            else if (args[argi].equals("-n")) {
                if (++argi < args.length) {
                    argJobName = args[argi++];
                }
                else {
                    argi = -1;
                    break;
                }
            }
            else {
                argi = -1;
                break;
            }
        }

        if (argi > 0 && argi < args.length) {
            argDotnetExe = args[argi++];
        }

        if (argi != args.length || argDotnetExe == null) {
            System.out.println("error!");
            System.out.println("usage: submit <jar> [-d <directory>] [-n <job-name>] <dotnet-exe>");
            System.out.println("  <directory>:  optional local directory containing <dotnet-exe> and more");
            System.out.println("  <job-name>:   optional name of the job");
            System.out.println("  <dotnet-exe>: name of the dotnet executable");
            System.out.println("If <directory> is not specified, then <dotnet-exe> must be a");
            System.out.println("self-contained single-file dotnet executable; if <directory> is ");
            System.out.println("specified then <dotnet-exe> is relative to <directory>.");
            System.out.println("All paths are local to the submitting environment.");
            System.exit(1);
        }

        if (argDirectory == null) {
            File file = new File(argDotnetExe);
            if (file.isDirectory() || !file.exists()) {
                System.out.println("error! invalid <dotnet-exe>");
                System.exit(1);
            }
        }
        else {
            File directory = new File(argDirectory);
            if (!directory.isDirectory() || !directory.exists()) {
                System.out.println("error! invalid <directory>");
                System.exit(1);
            }
            File file = new File(argDirectory + File.separator + argDotnetExe);
            if (file.isDirectory() || !file.exists()) {
                System.out.println("error! could not find <dotnet-exe> in <directory>");
                System.exit(1);
            }
        }

        return new DotnetServiceConfig()
                .withDirectory(argDirectory)
                .withDotnetExe(argDotnetExe)
                .withJobName(argJobName);
    }
}
