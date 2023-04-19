package com.hazelcast.jet.dotnet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Stream;

// configures the dotnet service
public final class DotnetServiceConfig implements Serializable {

    // TODO: once tests are done, adopt 1 number and stick with it
    //private static final long serialVersionUID = 3127394993311485471L;
    private static final long serialVersionUID = -1865148793772904203L;

    private String jobName;
    private String dotnetDir;
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

    // path to the directory containing the dotnet executables
    public String getDotnetDir() {

        return dotnetDir;
    }
    public void setDotnetDir(String value) {

        dotnetDir = value;
    }
    public DotnetServiceConfig withDotnetDir(String value) {

        setDotnetDir(value);
        return this;
    }
    public String getDotnetDirId(String platform) {

        return "dotnet-" + platform + "-";
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

    public void configureJob(JobConfig jobConfig) {

        jobConfig.setName(jobName);

        // JobConfig:
        // attachDirectory "Adds the directory identified by the supplied pathname to the list
        // of files that will be available to the job while it's executing in the Jet cluster"
        //
        // ServiceFactory:
        // withAttachedDirectory "attaches a directory to this service factory under the given ID.
        // It will become a part of the Jet job and available to createContextFn() as
        // processorContext.attachedDirectory(id)"
        //
        // ProcessorSupplier.Context:
        // attachedDirectory "uses the supplied ID to look up a directory you attached to the current
        // Jet job. Creates a temporary directory with the same contents on the local cluster member
        // and returns the location of the created directory.

        // and then: attaching a directory ends up using IOUtil.packDirectoryIntoZip which does not
        // support subdirectories, so we cannot attach the whole directory containing all platforms,
        // we're going to have to include one directory per platform.

        // this code runs on the client which is submitting the job

        File[] directories = new File(dotnetDir).listFiles(File::isDirectory);
        if (directories != null) {
            for (File directory : directories) {
                String id = getDotnetDirId(directory.getName());
                jobConfig.attachDirectory(directory, id);
            }
        }
    }

    public void configureServiceFactory(ServiceFactory serviceFactory) {

        // FIXME understand
        // jobConfig.attachDirectory will ZIP a directory and send it along the submit,
        // processorContext.recreateAttachedDirectory will unzip a directory to disk
        // but,
        // what is serviceFactory.withAttachedDirectory doing exactly?
        // where is this code running? client or member?
        // TODO: if member, then dotnetDir is n/a and we must do better?

        // withAttachedDirectory wants the directory to exist and be a directory
        // how?!
        // it just adds the (id, directory) into a map in the service factory
        // which ComputeStateImplBase then attach to the pipeline impl
        // and re-used by AbstractJetInstance to re-attach the files to the job config it creates
        //
        // (I am lost already)

        File[] directories = new File(dotnetDir).listFiles(File::isDirectory);
        if (directories != null) {
            for (File directory : directories) {
                String id = getDotnetDirId(directory.getName());
                serviceFactory.withAttachedDirectory(id, directory);
            }
        }
    }
}
