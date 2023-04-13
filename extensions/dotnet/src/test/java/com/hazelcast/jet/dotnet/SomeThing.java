package com.hazelcast.jet.dotnet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class SomeThing {

    private int value;

    public void setValue(int value) {

        this.value = value;
    }

    public int getValue() {

        return this.value;
    }

    @Override
    public String toString() {
        return "SomeThing(Value=" + value + ")";
    }
}

