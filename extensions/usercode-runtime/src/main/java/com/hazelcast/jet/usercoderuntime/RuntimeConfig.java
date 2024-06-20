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

import java.util.Properties;

/**
 *
 */
public class RuntimeConfig {

    private RuntimeType runtimeType;
    private RuntimeTransportType communicationType;
    private String imageName;
    private int port;
    private HazelcastProperties properties = new HazelcastProperties(new Properties());

    /**
     * @return
     */
    public HazelcastProperties getProperties() {
        return properties;
    }

    /**
     * @param properties
     */
    public RuntimeConfig setProperties(HazelcastProperties properties) {
        this.properties = Preconditions.isNotNull(properties, "properties");
        return this;
    }

    /**
     * @return
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port
     */
    public RuntimeConfig setPort(int port) {
        this.port = Preconditions.checkPortValid(port);
        return this;
    }

    /**
     * @return
     */
    public RuntimeTransportType getCommunicationType() {
        return communicationType;
    }

    /**
     * @param communicationType
     */
    public RuntimeConfig setCommunicationType(RuntimeTransportType communicationType) {
        this.communicationType = communicationType;
        return this;
    }

    /**
     * @return
     */
    public RuntimeType getRuntimeType() {
        return runtimeType;
    }

    /**
     * @param runtimeType
     */
    public RuntimeConfig setRuntimeType(RuntimeType runtimeType) {
        this.runtimeType = runtimeType;
        return this;
    }

    /**
     * @return
     */
    public String getImageName() {
        return imageName;
    }


    /**
     * @param imageName
     */
    public RuntimeConfig setImageName(String imageName) {
        this.imageName = Preconditions.checkHasText(imageName, "imageName");
        return this;
    }
}
