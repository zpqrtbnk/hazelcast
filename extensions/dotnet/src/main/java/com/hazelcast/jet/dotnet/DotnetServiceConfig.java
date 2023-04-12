package com.hazelcast.jet.dotnet;

import java.io.Serializable;

// configures the dotnet service
public final class DotnetServiceConfig implements Serializable {

    private String dotnetPath;
    private String dotnetExe;
    private String methodName;
    private int maxConcurrentOps = 1;
    private int localParallelism = 1;
    private boolean preserveOrder = true;

    // path to the directory containing the dotnet executable
    public String getDotnetPath() {

        return dotnetPath;
    }
    public void setDotnetPath(String value) {

        dotnetPath = value;
    }
    public DotnetServiceConfig withDotnetPath(String value) {

        setDotnetPath(value);
        return this;
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
}
