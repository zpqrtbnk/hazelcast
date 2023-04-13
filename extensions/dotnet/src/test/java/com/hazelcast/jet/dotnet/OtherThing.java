package com.hazelcast.jet.dotnet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class OtherThing {

    private String value;

    public void setValue(String value) {

        this.value = value;
    }

    public String getValue() {

        return this.value;
    }

    @Override
    public String toString() {
        return "OtherThing(Value=" + value + ")";
    }
}

