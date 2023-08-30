/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.usercode;

import java.util.concurrent.Future;

// represents a UserCode service
// - starts a UserCode runtime
// - stops a UserCode runtime
// - invokes a UserCode runtime
public interface UserCodeService {

    // starts a runtime
    // name: the unique name of the runtime
    // startInfo: arguments for the runtime creation
    // returns: the runtime
    Future<UserCodeRuntime> startRuntime(String name, UserCodeRuntimeInfo startInfo) throws UserCodeException;

    // destroys a runtime
    // runtime: the runtime
    Future<Void> destroyRuntime(UserCodeRuntime runtime);
}


