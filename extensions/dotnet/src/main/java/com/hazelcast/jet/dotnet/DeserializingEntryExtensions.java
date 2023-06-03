/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.journal.DeserializingEntry;
import com.hazelcast.internal.serialization.Data;

import java.lang.reflect.Field;

// provides utilities for dealing with DeserializingEntry instances
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

    // gets the key as Data
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

    // gets the value as Data
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

    // creates a new DeserializingEntry instance
    // by copying the SerializationService from another entry
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
