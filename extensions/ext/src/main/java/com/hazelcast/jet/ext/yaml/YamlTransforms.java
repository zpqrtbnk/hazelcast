package com.hazelcast.jet.ext.yaml;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.ext.compile.InMemoryFileManager;
import com.hazelcast.jet.ext.compile.JavaSourceFromString;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.logging.ILogger;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

public class YamlTransforms implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {

        jobBuilder.registerTransform("identity", YamlTransforms::identity);
        jobBuilder.registerTransform("lambda", YamlTransforms::lambda);
    }

    private static Object identity(Object stageContext, String name, YamlMapping properties, ILogger logger) {

        return stageContext;
    }

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
}
