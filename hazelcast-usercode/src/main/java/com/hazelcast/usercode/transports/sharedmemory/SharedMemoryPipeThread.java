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

package com.hazelcast.usercode.transports.sharedmemory;

import java.util.concurrent.TimeUnit;


// https://jenkov.com/tutorials/java-concurrency/creating-and-starting-threads.html

public final class SharedMemoryPipeThread extends Thread {

    private final SharedMemoryPipe pipe;
    private final int spinDelay;
    private boolean running;

    public SharedMemoryPipeThread(SharedMemoryPipe pipe, int spinDelay) {
        super("hazelcast-shm-" + pipe.getUniqueId());

        this.pipe = pipe;
        this.spinDelay = spinDelay;
    }

    public void run() {

        running = true;
        while (running) {

            boolean spin = spinDelay > 0;

            try {
                // try to send one message to the pipe
                // don't spin if a message was sent
                spin &= !pipe.send();

                // try to receive one message from the pipe
                // don't spin if a message was received
                spin &= !pipe.receive();
            }
            catch (Exception ex) {
                this.pipe.fail(ex);
                running = false;
                spin = false;
            }

            // spin if needed
            if (spin) {

                try {
                    TimeUnit.MILLISECONDS.sleep(spinDelay);
                }
                catch (InterruptedException ex) {
                    running = false;
                }
            }
        }
    }

    public void stopAndJoin() {
        running = false;
        try {
            this.join();
        }
        catch (InterruptedException e) {
            // FIXME this is probably a bad idea
        }
    }
}