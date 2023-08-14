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

/**
 *
 */
public class RuntimeConfig {

    private UserCodeRuntimeType runtimeType;
    private UserCodeCommunicationType communicationType;
    private String imageName;
    private int listeningPort;

    /**
     *
     * @return
     */
    public int getListeningPort() {
        return listeningPort;
    }

    /**
     *
     * @param listeningPort
     */
    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }

    /**
     *
     * @return
     */
    public UserCodeCommunicationType getCommunicationType() {
        return communicationType;
    }

    /**
     *
     * @param communicationType
     */
    public void setCommunicationType(UserCodeCommunicationType communicationType) {
        this.communicationType = communicationType;
    }

    /**
     *
     * @return
     */
    public UserCodeRuntimeType getRuntimeType() {
        return runtimeType;
    }

    /**
     *
     * @param runtimeType
     */
    public void setRuntimeType(UserCodeRuntimeType runtimeType) {
        this.runtimeType = runtimeType;
    }

    /**
     *
     * @return
     */
    public String getImageName() {
        return imageName;
    }


    /**
     *
     * @param imageName
     */
    public void setImageName(String imageName) {
        this.imageName = imageName;
    }
}
