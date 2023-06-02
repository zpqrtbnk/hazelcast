package com.hazelcast.jet.yaml;

@FunctionalInterface
public interface JobBuilderFunction2<T, U, R> {

    R apply(T t, U u) throws JobBuilderException;
}

