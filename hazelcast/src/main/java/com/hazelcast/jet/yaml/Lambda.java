package com.hazelcast.jet.yaml;

public interface Lambda<T, R> {

    R apply(T o);
}
