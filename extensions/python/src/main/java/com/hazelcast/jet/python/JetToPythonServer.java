/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.python;

import com.hazelcast.oop.service.ServiceProcess;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;

import static com.hazelcast.jet.python.PythonService.MAIN_SHELL_SCRIPT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

class JetToPythonServer {
    private final PythonServiceContext serviceContext;
    private final File baseDir;
    private ILogger logger;
    private ServiceProcess pythonProcess;

    JetToPythonServer(@Nonnull Path baseDir, @Nonnull ILogger logger, PythonServiceContext serviceContext) {
        this.baseDir = baseDir.toFile();
        this.logger = logger;
        this.serviceContext = serviceContext;
    }

    int start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

            pythonProcess = new ServiceProcess("python-jet", baseDir, serviceContext.getLoggingService())
                    .command("/bin/sh", "-c", String.format("./%s %d", MAIN_SHELL_SCRIPT, serverSocket.getLocalPort()))
                    .start();

            serverSocket.setSoTimeout((int) SECONDS.toMillis(2));
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream(), UTF_8))) {
                        int serverPort = Integer.parseInt(reader.readLine());
                        logger.info("Python process " + pythonProcess.pid() + " listening on port " + serverPort);
                        return serverPort;
                    }
                } catch (SocketTimeoutException e) {
                    if (!pythonProcess.isAlive()) {
                        throw new IOException("Python process died before completing initialization", e);
                    }
                }
            }
        }
    }

    void stop() {

        pythonProcess.write("stop");
        pythonProcess.stop();
    }
}
