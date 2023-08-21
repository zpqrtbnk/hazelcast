package com.hazelcast.usercode;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.jobbuilder.InfoMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// contains info required for starting a runtime
public final class UserCodeRuntimeInfo extends InfoMap implements Serializable {

    private final Map<String, String> resourceDirectories = new HashMap<>();
    private final String platform;

    private ProcessorSupplier.Context processorContext;

    public UserCodeRuntimeInfo(InfoMap info) {
        super(info.getSource());
        this.platform = determinePlatform();
    }

    public String getPlatform() {
        return platform;
    }

    public void setProcessorContext(ProcessorSupplier.Context processorContext) {
        this.processorContext = processorContext;
    }

    private String getOrRecreateResourceDirectory(String resourceId) {

        // cache
        String directory = resourceDirectories.get(resourceId);
        if (directory != null) return directory;

        // recreate resource
        Map<String, ResourceConfig> resources = processorContext.jobConfig().getResourceConfigs();
        ResourceConfig resource = resources.get(resourceId);
        if (resource == null) {
            throw new UserCodeException("Missing resource with id '" + resourceId + "'.");
        }
        if (resource.getResourceType() != ResourceType.DIRECTORY) {
            throw new UserCodeException("Resource with id '" + resourceId + "' is not a directory.");
        }
        directory = processorContext.recreateAttachedDirectory(resourceId).toString();
        resourceDirectories.put(resourceId, directory);
        return directory;
    }

    public String expand(String source, Map<String, String> values) {

        // two-phase replacement, so that we can support
        // {@dotnet-{PLATFORM}}
        //
        // but we do *not* support {FOO_{BAR}}

        String result = replace("(.|^)\\{([A-Za-z0-9_\\-]*)\\}", source, m -> {

            // '{{whatever}' -> skip
            if (m.group(1).equals("{")) return m.group();

            // '{whatever}' -> replacement for 'whatever'
            String value = values.get(m.group(2));
            if (value != null) return m.group(1) + value;

            // '{whatever}' -> '{whatever}' (no replacement)
            return m.group();
        });

        result = replace("(.|^)\\{(@[A-Za-z0-9_\\-]*)\\}", result, m -> {

            // '{{@whatever}' -> skip
            if (m.group(1).equals("{")) return m.group();

            // '{@whatever}' -> path to resource 'whatever'
            String value = getOrRecreateResourceDirectory(m.group(2).substring(1));
            if (value != null) return m.group(1) + value;

            // '{@whatever}' -> '{@whatever}' (no replacement)
            return m.group();
        });

        // '{{' -> '{'
        result = result.replace("{{", "{");

        return result;
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

    private static String determinePlatform() {
        // note: missing other OS (solaris...) here
        String os =
            OsHelper.isWindows() ? "win" :
            OsHelper.isLinux() ? "linux" :
            OsHelper.isMac() ? "osx" :
            "any";

        // note: missing other architectures (aarch64, ppc...) here
        String arch = System.getProperty("os.arch");
        if (arch.equals("amd64")) arch = "x64";
        if (arch.equals("x86_32")) arch = "x86";
        if (arch.equals("x86_64")) arch = "x64";

        return os + "-" + arch;
    }
}
