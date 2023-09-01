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

package com.hazelcast.jet.jobbuilder;

import java.io.Serializable;
import java.util.*;

public class JobBuilderInfoMap implements Serializable {
    private final Map<String, Object> source;

    public JobBuilderInfoMap() {
        this(new HashMap<>());
    }

    public JobBuilderInfoMap(Map<String, Object> source) {
        this.source = source;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public boolean hasChild(String name) {
        return source.containsKey(name);
    }

    public void setChild(String name, Object value) {
        source.put(name, value);
    }

    public boolean childIsMap(String name) {
        Object value = source.get(name);
        return value instanceof Map<?,?>;
    }

    public JobBuilderInfoMap childAsMap(String name) {
        return childAsMap(name, true);
    }

    public JobBuilderInfoMap childAsMap(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof Map<?,?>)) throw new IllegalStateException("Child with name '" + name + "' is not Map<,> but " + value.getClass() + ".");
        return new JobBuilderInfoMap((Map<String, Object>) value);
    }

    public boolean childIsList(String name) {
        Object value = source.get(name);
        return value instanceof List<?>;
    }

    public JobBuilderInfoList childAsList(String name) {
        return childAsList(name, true);
    }

    public JobBuilderInfoList childAsList(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof List<?>)) throw new IllegalStateException("Child with name '" + name + "' is not List<> but " + value.getClass() + ".");
        return new JobBuilderInfoList((List<Object>) value);
    }

    public boolean childIsString(String name) {
        Object value = source.get(name);
        return value instanceof String;
    }

    public String childAsString(String name) {
        return childAsString(name, true);
    }

    public String childAsString(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof String)) throw new IllegalStateException("Child with name '" + name + "' is not String but " + value.getClass() + ".");
        return (String) value;
    }

    public String childAsString(String name, String defaultValue) {
        String value = childAsString(name, false);
        return value == null ? defaultValue : value;
    }

    public boolean childIsUUID(String name) {
        Object value = source.get(name);
        return value instanceof UUID;
    }

    public UUID childAsUUID(String name) {
        return childAsUUID(name, true);
    }

    public UUID childAsUUID(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof UUID)) throw new IllegalStateException("Child with name '" + name + "' is not UUID but " + value.getClass() + ".");
        return (UUID) value;
    }

    public UUID childAsUUID(String name, UUID defaultValue) {
        UUID value = childAsUUID(name, false);
        return value == null ? defaultValue : value;
    }

    public boolean childIsBoolean(String name) {
        Object value = source.get(name);
        return value instanceof Boolean;
    }

    public Boolean childAsBoolean(String name) {
        return childAsBooleanRequired(name, true);
    }

    public Boolean childAsBooleanRequired(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof Boolean)) throw new IllegalStateException("Child with name '" + name + "' is not Boolean but " + value.getClass() + ".");
        return (Boolean) value;
    }

    public Boolean childAsBoolean(String name, boolean defaultValue) {
        Boolean value = childAsBooleanRequired(name, false);
        return value == null ? defaultValue : value;
    }

    public boolean childIsInteger(String name) {
        Object value = source.get(name);
        return value instanceof Integer;
    }

    public Integer childAsInteger(String name) {
        return childAsInteger(name, true);
    }

    public Integer childAsInteger(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof Integer)) throw new IllegalStateException("Child with name '" + name + "' is not Integer but " + value.getClass() + ".");
        return (Integer) value;
    }

    public Integer childAsInteger(String name, Integer defaultValue) {
        Integer value = childAsInteger(name, false);
        return value == null ? defaultValue : value;
    }

    public boolean childIsLong(String name) {
        Object value = source.get(name);
        return value instanceof Long;
    }

    public Long childAsLong(String name) {
        return childAsLong(name, true);
    }

    public Long childAsLong(String name, boolean required) {
        Object value = source.get(name);
        if (value == null) {
            if (required) throw new IllegalArgumentException("No child with name '" + name + "'.");
            return null;
        }
        if (!(value instanceof Long)) throw new IllegalStateException("Child with name '" + name + "' is not Long but " + value.getClass() + ".");
        return (Long) value;
    }

    public Long childAsLong(String name, Long defaultValue) {
        Long value = childAsLong(name, false);
        return value == null ? defaultValue : value;
    }

    public String uniqueChildName() {
        Set<String> keys = source.keySet();
        if (keys.size() != 1) {
			String children = String.join(",", source.keySet());
			throw new IllegalStateException("Expected unique child, got: " + children + ".");
		}
        return (String) keys.toArray()[0];
    }
}
