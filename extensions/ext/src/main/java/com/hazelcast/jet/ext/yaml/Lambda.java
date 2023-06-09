package com.hazelcast.jet.ext.yaml;
public interface Lambda<T, R> {

    public R apply(T o);
}
