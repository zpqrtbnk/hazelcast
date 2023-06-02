package com.hazelcast.jet.yaml;

@FunctionalInterface
public interface JobBuilderFunction4<T, U, V, W, R> {

    R apply(T t, U u, V v, W w) throws JobBuilderException;
}
