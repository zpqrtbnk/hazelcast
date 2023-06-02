package com.hazelcast.jet.yaml;

public interface JetExtension {

    void register(JobBuilder jobBuilder);
}
