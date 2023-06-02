package com.hazelcast.jet.yaml;

@FunctionalInterface
public interface JobBuilderFunction3<T, U, V, R> {

    R apply(T t, U u, V v) throws JobBuilderException;
}

