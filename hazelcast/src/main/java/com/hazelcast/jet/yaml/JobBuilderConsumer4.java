package com.hazelcast.jet.yaml;

@FunctionalInterface
public interface JobBuilderConsumer4<T, U, V, W> {

    void accept(T t, U u, V v, W w) throws JobBuilderException;
}
