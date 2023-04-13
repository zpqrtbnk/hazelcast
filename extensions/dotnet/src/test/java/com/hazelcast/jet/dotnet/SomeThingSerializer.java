package com.hazelcast.jet.dotnet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class SomeThingSerializer implements CompactSerializer<SomeThing> {

    @Nonnull
    @Override
    public SomeThing read(@Nonnull CompactReader reader) {

        SomeThing object = new SomeThing();
        object.setValue(reader.readInt32("value"));
        return object;
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull SomeThing object) {

        writer.writeInt32("value", object.getValue());
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
