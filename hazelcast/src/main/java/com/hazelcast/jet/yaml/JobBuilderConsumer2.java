package com.hazelcast.jet.yaml;

@FunctionalInterface
public interface JobBuilderConsumer2<T, U> {

    void accept(T t, U u) throws JobBuilderException;
}

