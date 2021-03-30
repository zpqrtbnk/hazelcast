package com.hazelcast.durableexecutor.impl;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import jdk.nashorn.internal.codegen.CompilerConstants;
import net.sf.jni4net.Bridge;

import java.io.IOException;
import java.util.concurrent.Callable;

public class NetCallable implements IdentifiedDataSerializable, Callable<Object> {
    private String className;

    public NetCallable(String className) {
        this.className = className;
    }

    public NetCallable() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(className);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readString();
    }

    @Override
    public int getFactoryId() {
        return DurableExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.NET_CALLABLE;
    }

    @Override
    public Object call() throws Exception {
        Bridge.init();
        Callable<Object> instance = ClassLoaderUtil.newInstance(null, className);
        return instance.call();
    }
}
