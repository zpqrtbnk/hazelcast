package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;

import java.lang.reflect.Field;

public final class DeserializingEntryExtensions {

    private static boolean initialized;
    private static Field dataKey;
    private static Field dataValue;
    private static Field serializationService;

    private DeserializingEntryExtensions() { }

    private static void initialize() {

        if (initialized) {
            return;
        }

        try {
            dataKey = DeserializingEntry.class.getDeclaredField("dataKey");
            dataKey.setAccessible(true);

            dataValue = DeserializingEntry.class.getDeclaredField("dataValue");
            dataValue.setAccessible(true);

            serializationService = DeserializingEntry.class.getDeclaredField("serializationService");
            serializationService.setAccessible(true);
        }
        catch (Exception e) {
            // should not happen (famous last words)
        }

        initialized = true;
    }

    public static <TK, TV> Data getDataKey(DeserializingEntry<TK, TV> entry) {

        // is private
        //return entry.dataKey;

        initialize();
        try {
            return (Data) dataKey.get(entry);
        }
        catch (Exception e) {
            return null; // meh
        }
    }

    public static <TK, TV> Data getDataValue(DeserializingEntry<TK, TV> entry) {

        // is private
        //return entry.dataValue;

        initialize();
        try {
            return (Data) dataValue.get(entry);
        }
        catch (Exception e) {
            return null; // meh
        }
    }

    public static <TK, TV> DeserializingEntry<TK, TV> createNew(DeserializingEntry<?,?> entry, Data dataKey, Data dataValue) {
        DeserializingEntry<TK, TV> result = new DeserializingEntry<TK, TV>(dataKey, dataValue);

        // is private
        //result.serializationService = entry.serializationService;

        initialize();
        try {
            serializationService.set(result, serializationService.get(entry));
        }
        catch (Exception e) {
            // meh
        }

        return result;
    }
}
