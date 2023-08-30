package com.hazelcast.usercode.experiments;

public interface Lambda<T, R> {

    R apply(T o);
}
