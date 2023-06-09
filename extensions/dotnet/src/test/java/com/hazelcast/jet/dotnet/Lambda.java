package com.hazelcast.jet.dotnet;

import java.util.function.Function;

public interface Lambda<T, R> {
    //R apply(T arg0);
    Function<T,R> getFunction();
}
