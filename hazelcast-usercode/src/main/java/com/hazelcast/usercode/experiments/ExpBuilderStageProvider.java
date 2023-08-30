package com.hazelcast.usercode.experiments;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.jobbuilder.*;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.usercode.compile.InMemoryFileManager;
import com.hazelcast.usercode.compile.JavaSourceFromString;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.util.Collections;
import java.util.UUID;

public class ExpBuilderStageProvider implements JobBuilderStageProvider {

    @Override
    public SourceStage[] getSources() {
        return new SourceStage[0];
    }

    @Override
    public TransformStage[] getTransforms() {
        return new TransformStage[] {
                new TransformStage("lambda", ExpBuilderStageProvider::lambda)
        };
    }

    @Override
    public SinkStage[] getSinks() {
        return new SinkStage[0];
    }

    // highly experimental and won't work on viridian of course
    private static Object lambda(Object stageContext, String name, JobBuilderInfoMap definition, ILogger logger) throws JobBuilderException {

        String expression = definition.childAsString("expr");
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
}
