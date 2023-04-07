package com.hazelcast.jet.dotnet;

import java.io.Serializable;

public class DotnetServiceConfig implements Serializable {

    private String dotnetPath;
    private String dotnetExe;
    private String methodName;
    private String pipeName = "dotnet-jet";
    private int maxConcurrentOps = 4;
    private int localParallelism = 2;
    private boolean preserveOrder = true;

    public String getDotnetPath() { return dotnetPath; }
    public void setDotnetPath(String value) { dotnetPath = value; }
    public DotnetServiceConfig withDotnetPath(String value) {
        setDotnetPath(value);
        return this;
    }
    public String getDotnetExe() { return dotnetExe; }
    public void setDotnetExe(String value) { dotnetExe = value; }
    public DotnetServiceConfig withDotnetExe(String value) {
        setDotnetExe(value);
        return this;
    }
    public String getMethodName() { return methodName; }
    public void setMethodName(String name) { methodName = name; }
    public DotnetServiceConfig withMethodName(String name) {
        setMethodName(name);
        return this;
    }
    public String getPipeName() { return pipeName; }
    public void setPipeName(String name) { pipeName = name; }
    public DotnetServiceConfig withPipeName(String name) {
        setPipeName(name);
        return this;
    }
    public int getMaxConcurrentOps() { return maxConcurrentOps; }
    public void setMaxConcurrentOps(int value) { maxConcurrentOps = value; }
    public DotnetServiceConfig withMaxConcurrentOps(int value) {
        setMaxConcurrentOps(value);
        return this;
    }
    public int getLocalParallelism() { return localParallelism; }
    public void setLocalParallelism(int value) { localParallelism = value; }
    public DotnetServiceConfig withLocalParallelism(int value) {
        setLocalParallelism(value);
        return this;
    }
    public DotnetServiceConfig withParallelism(int processors, int operations) {
        return this
                .withLocalParallelism(processors)
                .withMaxConcurrentOps(operations);
    }
    public boolean getPreserveOrder() { return preserveOrder; }
    public void setPreserveOrder(boolean value) { preserveOrder = value; }
    public DotnetServiceConfig withPreserveOrder(boolean value) {
        setPreserveOrder(value);
        return this;
    }
}
