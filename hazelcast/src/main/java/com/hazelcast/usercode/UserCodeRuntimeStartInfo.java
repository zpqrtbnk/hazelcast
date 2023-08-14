package com.hazelcast.usercode;

import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.oop.service.ServiceProcess;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// contains info required for starting a runtime
public final class UserCodeRuntimeStartInfo implements Serializable {

    private ProcessorSupplier.Context processorContext;
    private final Map<String, Object> info = new HashMap<>();

    public void setProcessorContext(ProcessorSupplier.Context processorContext) {
        this.processorContext = processorContext;
    }

    public void set(String name, Object value) {
        info.put(name, value);
    }

    public <T> T get(String name) {
        Object objectValue = info.get(name);
        if (objectValue == null) throw new IllegalArgumentException("no value with name '" + name + "'");
        return (T) objectValue; // duh, java can only do unsafe casts
    }

    public <T> T get(String name, T defaultValue) {
        Object objectValue = info.get(name);
        if (objectValue == null) return defaultValue;
        return (T) objectValue; // duh, java can only do unsafe casts
    }

    public String recreateResourceDirectory(String resourceId) {

        // FIXME should we do this here, or?
        Map<String, String> values = new HashMap<>();
        values.put("PLATFORM", ServiceProcess.getPlatform());

        resourceId = replace("(.|^)\\$([A-Z_]*)", resourceId, m -> {
            if (m.group(1).equals("\\")) return "$" + m.group(2);
            String value = values.get(m.group(2));
            if (value != null) return m.group(1) + value;
            return m.group();
        });

        // recreate resource
        Map<String, ResourceConfig> resources = processorContext.jobConfig().getResourceConfigs();
        ResourceConfig resource = resources.get(resourceId);
        if (resource == null) {
            throw new UserCodeException("Missing resource with id '" + resourceId + "'.");
        }
        if (resource.getResourceType() != ResourceType.DIRECTORY) {
            throw new UserCodeException("Resource with id '" + resourceId + "' is not a directory.");
        }
        return processorContext.recreateAttachedDirectory(resourceId).toString();
    }

    private static String replace(String pattern, String source, Function<Matcher, String> handler) {
        return replace(Pattern.compile(pattern), source, handler);
    }

    private static String replace(Pattern pattern, String source, Function<Matcher, String> handler) {
        StringBuilder sb = null;
        Matcher matcher = pattern.matcher(source);
        int pos = 0;
        while (matcher.find()) {
            if (sb == null) {
                sb = new StringBuilder();
            }
            String replacement = handler.apply(matcher);
            sb.append(source, pos, matcher.start());
            sb.append(replacement);
            pos = matcher.end();
        }
        if (sb == null) {
            return source;
        }
        sb.append(source, pos, source.length());
        return sb.toString();
    }
}
