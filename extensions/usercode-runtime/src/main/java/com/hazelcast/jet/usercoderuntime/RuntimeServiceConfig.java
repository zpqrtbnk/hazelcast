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

package com.hazelcast.jet.usercoderuntime;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.jet.usercoderuntime.impl.UserCodeRuntimeService;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Properties;

/**
 * The class represents the configurations for {@link UserCodeRuntimeService}.
 *
 * @since 5.4
 */
public class RuntimeServiceConfig implements Serializable {
    private static final int DEFAULT_GRPC_PORT = 55001;
    private String controllerAddress = "usercodecontroller";
    private int controllerGrpcPort = DEFAULT_GRPC_PORT;
    private HazelcastProperties properties;

    /**
     * Initializes a new {@link RuntimeServiceConfig}. It holds configurations for
     * the {@link UserCodeRuntimeService} to controller pod for user code runtimes.
     *
     * @since 5.4
     */
    public RuntimeServiceConfig() {
        properties = new HazelcastProperties(new Properties());
    }

    /**
     * Sets {@link HazelcastProperties} of the Service.
     *
     * @since 5.4
     */
    public RuntimeServiceConfig setProperties(HazelcastProperties properties) {
        this.properties = Preconditions.isNotNull(properties, "properties");
        return this;
    }

    /**
     * Gets {@link HazelcastProperties} of the Service.
     *
     * @since 5.4
     */
    public HazelcastProperties getProperties() {
        return properties;
    }

    /**
     * Returns address of the controller.
     *
     * @since 5.4
     */
    public String getControllerAddress() {
        return controllerAddress;
    }

    /**
     * Sets address of the controller.
     *
     * @since 5.4
     */
    public RuntimeServiceConfig setControllerAddress(@Nonnull String controllerAddress) {
        this.controllerAddress = Preconditions.checkHasText(controllerAddress,
                "Address of the user code runtime controller cannot be null or empty.");
        return this;
    }

    /**
     * Returns grpc port of the controller.
     *
     * @since 5.4
     */
    public int getControllerGrpcPort() {
        return controllerGrpcPort;
    }

    /**
     * Sets grpc port of the controller.
     *
     * @since 5.4
     */
    public RuntimeServiceConfig setControllerGrpcPort(int controllerGrpcPort) {
        this.controllerGrpcPort = Preconditions.checkPortValid(controllerGrpcPort);
        return this;
    }


}
