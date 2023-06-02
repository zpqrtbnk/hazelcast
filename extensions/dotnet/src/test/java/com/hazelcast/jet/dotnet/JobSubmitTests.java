package com.hazelcast.jet.dotnet;

import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.jet.impl.deployment.IMapInputStream;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.yaml.JobBuilder;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobRepository.fileKeyName;
import static com.hazelcast.jet.impl.util.IOUtil.unzip;

public class JobSubmitTests {

//    @Test
//    public void test() throws Exception {
//
//        InputStream inputStream = JobSubmitTests.class.getClassLoader().getResourceAsStream("yaml-test-job-1.yaml");
//        YamlNode root = YamlLoader.load(inputStream);
//        JobBuilder jobBuilder = new JobBuilder();
//        jobBuilder.parse(root);
//        Pipeline pipeline = jobBuilder.getPipeline();
//        pipeline.toDag(); // validate the pipeline
//    }

    @Test
    public void unzip() throws Exception {

        String path = "C:\\Users\\sgay\\Code\\hazelcast-jet-dotnet\\temp";
        //IMapInputStream inputStream = new IMapInputStream(map, fileKeyName(id));
        InputStream inputStream = Files.newInputStream(Paths.get(path, "0000001e-win-x64.zip"));
        Path destPath = Paths.get(path);
        unzip(inputStream, destPath);
        //return destPath.toFile();
    }

    private static void unzip(InputStream is, Path targetDir) throws IOException {
        targetDir = targetDir.toAbsolutePath();
        try (ZipInputStream zipIn = new ZipInputStream(is)) {
            for (ZipEntry ze; (ze = zipIn.getNextEntry()) != null; ) {
                Path resolvedPath = targetDir.resolve(ze.getName()).normalize();
                if (!resolvedPath.startsWith(targetDir)) {
                    // see: https://snyk.io/research/zip-slip-vulnerability
                    throw new RuntimeException("Entry with an illegal path: "
                            + ze.getName());
                }
                if (ze.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    Path dir = resolvedPath.getParent();
                    assert dir != null : "null parent: " + resolvedPath;
                    Files.createDirectories(dir);
                    Files.copy(zipIn, resolvedPath);
                }
            }
        }
    }
}
