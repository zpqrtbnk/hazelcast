package com.hazelcast.jet.dotnet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class SomeThingSerializer implements CompactSerializer<SomeThing> {

    @Nonnull
    @Override
    public SomeThing read(@Nonnull CompactReader reader) {
        return new SomeThing();
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull SomeThing object) {

    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "some-thing";
    }

    @Nonnull
    @Override
    public Class<SomeThing> getCompactClass() {
        return SomeThing.class;
    }
}
