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
package com.hazelcast.jet.ext;

import com.hazelcast.jet.JetException;

// represents the exception that is thrown when a Jet service configuration is invalid
public class JetServiceConfigurationException extends JetException {

    private static final long serialVersionUID = 1L;

    public JetServiceConfigurationException(String message) {
        super(message);
    }

    public JetServiceConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
