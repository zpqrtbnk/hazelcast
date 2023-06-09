package com.hazelcast.jet.dotnet;

import com.hazelcast.jet.dotnet.compile.*;
import org.junit.Test;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class JavaTests {

    //private <T,R> R consume(Function<T,R> f) {
    //    return f(1234);
    //}

    @Test
    public void Test2() throws Exception {

        final String qualifiedClassName = "com.hazelcast.jet.job_12345.Lambda_12";

        String source =
                "package com.hazelcast.jet.job_12345;\n" +
                "import java.util.function.Function;\n" +
                "import com.hazelcast.jet.dotnet.Lambda;\n" +
                "public final class Lambda_12 implements Lambda {\n" +
                "  private final Function<Object, Object> f = $$LAMBDA$$;\n" +
                "  //public Object apply(Object arg0) { return f.apply(arg0); }\n" +
                "  public Function<Object, Object> getFunction() { return f; }\n" +
                "}";
        source = source.replace("$$LAMBDA$$", "x -> x.toString()");
        System.out.println(source);
        JavaSourceFromString sourceString = new JavaSourceFromString(qualifiedClassName, source);

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

        StandardJavaFileManager standardFileManager
                = javaCompiler.getStandardFileManager(null, null, null);
        InMemoryFileManager fileManager
                = new InMemoryFileManager(standardFileManager);

        Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(sourceString);

        javaCompiler
                .getTask(null, fileManager, null, null, null, compilationUnits)
                .call();

        fileManager.close();

        ClassLoader classLoader = fileManager.getClassLoader(null);
        Class<?> clazz = classLoader.loadClass(qualifiedClassName);
        Lambda<Integer, String> lambda = (Lambda<Integer, String>) clazz.newInstance();

        // this is convoluted
        //String result = lambda.apply(12345);
        //System.out.println(result);

        // this is the easiest way
        // we could do .map(lambda.getFunction()) and be done with it!
        Function<Integer, String> f = lambda.getFunction();
        String result = f.apply(12345);
        System.out.println(result);

        // now... considering generics in Java...
        // do we even want to bother with the actual lambda arguments types ?!
        // YES! otherwise the lambda might just not compile!! eg string.length but NOT object.length

        // then
        // these lambdas are totally static / stateless
        // HOW CAN WE IMPLEMENT STATEFUL THINGS?
        // outside dotnet/python, don't try, use Java
        // in dotnet/python... is there a way to 'reset state' or something?
        // or stateful services?
    }

    @Test
    public void Test() throws Exception {

        // this test compiles some Java code at runtime
        // including a lambda that we're passing

        final String resourceName = "Foo1.java";
        final String qualifiedClassName = "com.hazelcast.foo.Foo1";

        InputStream inputStream = JavaTests.class.getClassLoader().getResourceAsStream(resourceName);
        //String source = inputStream.readAllBytes()... // Java 9
        String source = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        source = source.replace("$$LAMBDA$$", "x -> foo(x)");
        JavaSourceFromString sourceString = new JavaSourceFromString(qualifiedClassName, source);

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

        StandardJavaFileManager standardFileManager
                = javaCompiler.getStandardFileManager(null, null, null);
        InMemoryFileManager fileManager
                = new InMemoryFileManager(standardFileManager);

        Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(sourceString);

        javaCompiler
                .getTask(null, fileManager, null, null, null, compilationUnits)
                .call();

        // if !result ... see https://www.baeldung.com/java-string-compile-execute-code

        ClassLoader classLoader = fileManager.getClassLoader(null);
        Class<?> clazz = classLoader.loadClass(qualifiedClassName);

        // note: if the class implemented an interface we could invoke methods without reflection

        Method meh = clazz.getDeclaredMethod("meh");
        String res = (String) meh.invoke(clazz.newInstance(), new Object[] { });
        System.out.println(res);
    }
}

