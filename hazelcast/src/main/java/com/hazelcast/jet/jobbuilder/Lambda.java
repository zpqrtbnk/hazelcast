package com.hazelcast.jet.jobbuilder;

public interface Lambda<T, R> {

    R apply(T o);
}
