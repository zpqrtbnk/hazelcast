package com.hazelcast.jet.yaml;
public interface Lambda<T, R> {

    public R apply(T o);
}
