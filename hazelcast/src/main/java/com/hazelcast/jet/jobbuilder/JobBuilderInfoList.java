package com.hazelcast.jet.jobbuilder;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JobBuilderInfoList implements Serializable {

    private final List<Object> source;

    public JobBuilderInfoList(List<Object> source) {
        this.source = source;
    }

    public List<Object> getSource() {
        return source;
    }

    public int size() {
        return source.size();
    }

    private Object item(int index) {
        if (index < 0 || index >= source.size()) throw new IllegalArgumentException("No item with index '" + index + "'.");
        return source.get(index);
    }

    public JobBuilderInfoMap itemAsMap(int index) {
        Object value = item(index);
        if (value == null) {
            throw new IllegalArgumentException("Null child at name '" + index + "'.");
        }
        if (!(value instanceof Map<?,?>)) throw new IllegalStateException("Child at index '" + index + "' is not Map<,> but " + value.getClass() + ".");
        return new JobBuilderInfoMap((Map<String,Object>) value);
    }

    public String itemAsString(int index) {
        Object value = item(index);
        if (value == null) {
            throw new IllegalArgumentException("Null child at name '" + index + "'.");
        }
        if (!(value instanceof String)) throw new IllegalStateException("Child at index '" + index + "' is not String but " + value.getClass() + ".");
        return (String) value;
    }
}
