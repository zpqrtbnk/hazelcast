package com.hazelcast.jet.dotnet;

import com.hazelcast.jet.config.JobConfig;

import java.io.File;
import java.io.Serializable;

// configures the dotnet service
public final class DotnetServiceConfig implements Serializable {

    private String jobName;
    private String directory;
    private String dotnetExe;
    private String methodName;
    private int maxConcurrentOps = 1;
    private int localParallelism = 1;
    private boolean preserveOrder = true;

    // name of the job
    public String getJobName() {

        return jobName;
    }
    public void setJobName(String value) {

        jobName = value;
    }
    public DotnetServiceConfig withJobName(String value) {

        setJobName(value);
        return this;
    }

    // path to the directory containing the dotnet executable
    public String getDirectory() {

        return directory;
    }
    public void setDirectory(String value) {

        directory = value;
    }
    public DotnetServiceConfig withDirectory(String value) {

        setDirectory(value);
        return this;
    }
    public String getDirectoryId() {

        return createId(directory);
    }
    public boolean hasDirectory() {

        return directory != null;
    }

    // name of the dotnet executable
    public String getDotnetExe() {

        return dotnetExe;
    }
    public void setDotnetExe(String value) {

        dotnetExe = value;
    }
    public DotnetServiceConfig withDotnetExe(String value) {

        setDotnetExe(value);
        return this;
    }
    public String getDotnetExeId() {

        return createId(dotnetExe);
    }

    // name of the method that the dotnet executable should execute
    public String getMethodName() {

        return methodName;
    }
    public void setMethodName(String name) {

        methodName = name;
    }
    public DotnetServiceConfig withMethodName(String name) {

        setMethodName(name);
        return this;
    }

    // max. concurrent operations (operations per processor)
    public int getMaxConcurrentOps() {

        return maxConcurrentOps;
    }
    public void setMaxConcurrentOps(int value) {

        maxConcurrentOps = value;
    }
    public DotnetServiceConfig withMaxConcurrentOps(int value) {

        setMaxConcurrentOps(value);
        return this;
    }

    // local parallelism (processors per member)
    public int getLocalParallelism() {

        return localParallelism;
    }
    public void setLocalParallelism(int value) {

        localParallelism = value;
    }
    public DotnetServiceConfig withLocalParallelism(int value) {

        setLocalParallelism(value);
        return this;
    }

    // max. concurrent operations + local parallelism
    public DotnetServiceConfig withParallelism(int processors, int operations) {

        return this
                .withLocalParallelism(processors)
                .withMaxConcurrentOps(operations);
    }

    // whether to preserve order
    public boolean getPreserveOrder() {

        return preserveOrder;
    }
    public void setPreserveOrder(boolean value) {

        preserveOrder = value;
    }
    public DotnetServiceConfig withPreserveOrder(boolean value) {

        setPreserveOrder(value);
        return this;
    }

    public String getDotnetExeFullPath() {

        return directory == null
                ? dotnetExe
                : (directory + File.separator + dotnetExe);
    }

    public String getDotnetExeDirectory() {

        return directory == null
                ? new File(dotnetExe).getParent()
                : directory;
    }

    public void configureJob(JobConfig jobConfig) {

        jobConfig.setName(jobName);

        // attachDirectory (and File) "Adds the directory identified by the supplied pathname to the list
        // of files that will be available to the job while it's executing in the Jet cluster"

        if (directory != null) {
            jobConfig.attachDirectory(directory, createId(directory));
        }
        else {
            jobConfig.attachFile(dotnetExe, createId(dotnetExe));
        }
    }

    private String createId(String file) {

        return file.replace('\\', '/').replace('/', '-');
    }
}
