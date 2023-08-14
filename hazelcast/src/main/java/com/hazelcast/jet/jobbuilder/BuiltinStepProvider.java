package com.hazelcast.jet.jobbuilder;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.usercode.compile.InMemoryFileManager;
import com.hazelcast.usercode.compile.JavaSourceFromString;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.util.Collections;
import java.util.UUID;

public class BuiltinStepProvider implements StepProvider {

    @Override
    public SourceStep[] getSources() {
        return new SourceStep[] {
            new SourceStep("map-journal", BuiltinStepProvider::sourceMapJournal)
        };
    }

    @Override
    public TransformStep[] getTransforms() {
        return new TransformStep[] {
            new TransformStep("identity", BuiltinStepProvider::identity),
            new TransformStep("lambda", BuiltinStepProvider::lambda)
        };
    }

    @Override
    public SinkStep[] getSinks() {
        return new SinkStep[] {
            new SinkStep("map", BuiltinStepProvider::sinkMap)
        };
    }

    private static Object sourceMapJournal(Pipeline pipelineContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        String mapName = YamlUtils.getProperty(properties, "map-name");
        String initialPositionString = YamlUtils.getProperty(properties, "journal-initial-position");
        //JournalInitialPosition initialPosition = getProperty(properties, "journal-initial-position");
        JournalInitialPosition initialPosition = JournalInitialPosition.valueOf(initialPositionString);
        logger.fine("  map-name: " + mapName);
        logger.fine("  initial-position: " + initialPosition);
        StreamSource streamSource = Sources.mapJournal(mapName, initialPosition);
        if (streamSource == null) logger.fine("MEH source");
        if (pipelineContext == null) logger.fine("MEH pipelineContext");

        StreamSourceStage sourceStage = pipelineContext.readFrom(streamSource);
        StreamStage stage;

        String timestamps = YamlUtils.getProperty(properties, "timestamps", "NONE");
        switch (timestamps.toUpperCase()) {
            case "NONE":
                stage = sourceStage.withoutTimestamps();
                break;
            case "NATIVE":
                long nativeAllowedLag = YamlUtils.<Long>getProperty(properties, "timestamps-allowed-lag");
                stage = sourceStage.withNativeTimestamps(nativeAllowedLag);
                break;
            case "INGESTION":
                stage = sourceStage.withIngestionTimestamps();
                break;
            default:
                long lambdaAllowedLag = YamlUtils.<Long>getProperty(properties, "timestamps-allowed-lag");
                // TODO: we *should* handle the lambda somehow?
                throw new JobBuilderException("panic: lambda not supported");
        }

        return stage;
    }

    private static Object identity(Object stageContext, String name, YamlMapping properties, ILogger logger) {

        return stageContext;
    }

    // highly experimental and won't work on viridian of course
    private static Object lambda(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        String expression = YamlUtils.getProperty(properties, "expr");
        String classId = UUID.randomUUID().toString().replace("-", "");
        String className = "Lambda_" + classId;
        String qualifiedClassName = "com.hazelcast.jet.lambdas." + className;
        String source =
                "package com.hazelcast.jet.lambdas;\n" +
                        "import java.util.function.Function;\n" +
                        "import com.hazelcast.jet.ext.yaml.Lambda;\n" +
                        "public class " + className + " implements Lambda{\n" +
                        "  private final Function<Object, Object> f = " + expression + ";\n" +
                        "  @Override\n" +
                        "  public Object apply(Object o) {\n" +
                        "    return f.apply(o);\n" +
                        "  }\n" +
                        "}";

        JavaSourceFromString sourceString = new JavaSourceFromString(qualifiedClassName, source);

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

        StandardJavaFileManager standardFileManager
                = javaCompiler.getStandardFileManager(null, null, null);
        InMemoryFileManager fileManager
                = new InMemoryFileManager(standardFileManager);

        Iterable<? extends JavaFileObject> compilationUnits = Collections.singletonList(sourceString);

        javaCompiler
                .getTask(null, fileManager, null, null, null, compilationUnits)
                .call();

        // if !result ... see https://www.baeldung.com/java-string-compile-execute-code

        ClassLoader classLoader = fileManager.getClassLoader(null);

        FunctionEx mapFn;
        try  {
            Class<?> clazz = classLoader.loadClass(qualifiedClassName);
            Lambda instance = (Lambda) clazz.newInstance();
            mapFn = instance::apply;
        }
        catch (Exception e) {

            throw new JobBuilderException("panic: could not create mapFn");
        }

        return ((GeneralStage) stageContext).map(mapFn);
    }

    private static void sinkMap(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        String mapName = YamlUtils.getProperty(properties, "map-name");
        logger.fine("  map-name: " + mapName);
        if (stageContext instanceof GeneralStage) {
            // FIXME how can we check?
            ((GeneralStage)stageContext).writeTo(Sinks.map(mapName));
        }
        else {
            throw new JobBuilderException("panic");
        }
    }
}
