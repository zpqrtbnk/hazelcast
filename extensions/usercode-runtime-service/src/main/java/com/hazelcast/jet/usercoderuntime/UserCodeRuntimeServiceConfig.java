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

/**
 *
 */
package com.hazelcast.jet.usercoderuntime;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.util.Preconditions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 *
 */
public class UserCodeRuntimeServiceConfig implements Serializable {
    private static final int MAX_PORT_NUMBER = 65536;
    private BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn =
            NettyChannelBuilder::forAddress;

    private String controllerAddress;
    private int controllerPort;


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
    public void setControllerAddress(@Nonnull String controllerAddress) {
        this.controllerAddress = Preconditions.checkHasText(controllerAddress,
                "Address of the controller cannot be null or empty.");
    }

    /**
     * Returns grpc port of the controller.
     *
     * @since 5.4
     */
    public int getControllerPort() {
        return controllerPort;
    }

    /**
     * Sets grpc port of the controller.
     *
     * @since 5.4
     */
    public void setControllerPort(int controllerPort) {
        if (controllerPort < 0 || controllerPort > MAX_PORT_NUMBER) {
            throw new IllegalArgumentException("GRPC port of the controller is not valid.");
        }
        this.controllerPort = controllerPort;
    }

    /**
     * Returns the channel function for user code runtime controller, see {@link #setChannelFn}.
     *
     * @since 5.4
     */
    @Nonnull
    public BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn() {
        return channelFn;
    }

    /**
     * Sets the channel function. The function receives a host+port tuple, and
     * it's supposed to return a configured instance of {@link
     * ManagedChannelBuilder}. You can use this to configure the channel, for
     * example to configure the maximum message size etc.
     * <p>
     * The default value is {@link NettyChannelBuilder#forAddress
     * NettyChannelBuilder::forAddress}.
     *
     * @since 5.4
     */
    @Nonnull
    public UserCodeRuntimeServiceConfig setChannelFn(
            @Nonnull BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn
    ) {
        this.channelFn = Preconditions.isNotNull(channelFn, "channelFn");
        return this;
    }
}
