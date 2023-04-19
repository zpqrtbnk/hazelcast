package com.hazelcast.jet.dotnet;

import java.io.File;

// processes the submit parameters
public final class DotnetSubmit {

    private DotnetSubmit() { }

    // display submit usage
    private static void usage() {

        System.out.println("usage: submit <jar> [-d <dotnet-dir>] [-n <job-name>] [-x <dotnet-exe>]");
        System.out.println("  <dotnet-dir>: directory containing the dotnet executables");
        System.out.println("  <job-name>:   optional name of the job (default is 'dotjet')");
        System.out.println("  <dotnet-exe>: optional name of the dotnet executable");
        System.out.println("The <dotnet-dir> path is local to the submitting environment.");
        System.out.println("It must contain sub-directories named after platforms e.g. 'win-x64' which");
        System.out.println("in-turn must contain at least a file named <dotnet-exe> which is going to");
        System.out.println("be executed.");
        System.out.println("The <dotnet-exe> name does not need to have the '.exe' extension as the");
        System.out.println("job will adjust the extension depending on the platform.");
    }

    // processes the submit parameters
    public static DotnetServiceConfig getConfig(String[] args) {

        String argDotnetDir = null;
        String argJobName = "dotjet";
        String argDotnetExe = null;

        int argi = 0;
        while (argi < args.length && args[argi].startsWith("-")) {
            if (args[argi].equals("-d")) {
                if (++argi < args.length) {
                    argDotnetDir = args[argi++];
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
            else if (args[argi].equals("-x")) {
                if (++argi < args.length) {
                    argDotnetExe = args[argi++];
                }
                else {
                    argi = -1;
                    break;
                }
            }            else {
                argi = -1;
                break;
            }
        }

        if (argi != args.length || argDotnetDir == null || argDotnetExe == null) {
            System.out.println("error!");
            usage();
            System.exit(1);
        }

        // trim .exe extension if any
        if (argDotnetExe.endsWith(".exe")) {
            argDotnetExe = argDotnetExe.substring(0, argDotnetExe.length() - ".exe".length());
        }

        // ensure directory exists
        File directory = new File(argDotnetDir);
        if (!directory.exists() || !directory.isDirectory()) {
            System.out.println("error! invalid <directory>");
            System.exit(1);
        }

        // ensure platform executables exist
        int platformCount = 0;
        platformCount += EnsurePlatformExe("win-x64", argDotnetDir, argDotnetExe);
        platformCount += EnsurePlatformExe("linux-x64", argDotnetDir, argDotnetExe);
        if (platformCount == 0) {
            System.out.println("error! no platform found in <directory>");
            System.exit(1);
        }

        return new DotnetServiceConfig()
                .withDotnetDir(argDotnetDir)
                .withDotnetExe(argDotnetExe)
                .withJobName(argJobName);
    }

    private static int EnsurePlatformExe(String platform, String argDotnetDir, String argDotnetExe) {

        String platformDirRelative = File.separator + platform;
        File platformDir = new File(argDotnetDir + platformDirRelative);
        if (!platformDir.exists()) {
            return 0;
        }

        if (!platformDir.isDirectory()) {
            System.out.println("error! invalid <directory>" + platformDirRelative);
            System.exit(1);
        }
        String platformExeRelative = platformDirRelative + File.separator + argDotnetExe;
        File platformExe = new File(argDotnetDir + platformExeRelative);
        if (platformExe.exists() && !platformExe.isDirectory()) {
            return 1;
        }
        platformExe = new File(argDotnetDir + platformExeRelative + ".exe");
        if (platformExe.exists() && !platformExe.isDirectory()) {
            return 1;
        }

        System.out.println("error! invalid or missing executable <directory>" + platformExeRelative);
        System.exit(1);
        return 0;
    }
}
