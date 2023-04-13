package com.hazelcast.jet.dotnet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class OtherThingSerializer implements CompactSerializer<OtherThing> {

    @Nonnull
    @Override
    public OtherThing read(@Nonnull CompactReader reader) {


        OtherThing object = new OtherThing();
        object.setValue(reader.readString("value"));
        return object;
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull OtherThing object) {

        writer.writeString("value", object.getValue());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "other-thing";
    }

    @Nonnull
    @Override
    public Class<OtherThing> getCompactClass() {
        return OtherThing.class;
    }
}
