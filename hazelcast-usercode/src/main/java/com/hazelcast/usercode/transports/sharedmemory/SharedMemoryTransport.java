package com.hazelcast.usercode.transports.sharedmemory;

import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransport;
import com.hazelcast.usercode.UserCodeTransportReceiver;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// a UserCodeTransport that works over shared memory
public final class SharedMemoryTransport implements UserCodeTransport {

    // FIXME should be parameters & options
    private final int DATA_CAPACITY = 1024; // bytes
    private final int SPIN_DELAY = 4; // ms
    private final int OPEN_TIMEOUT = 5000; // ms

    private final UUID uniqueId;
    private UserCodeTransportReceiver receiver;

    private SharedMemoryPipe pipe;

    public SharedMemoryTransport(UUID uniqueId) {

        this.uniqueId = uniqueId;
    }

    public void setReceiver(UserCodeTransportReceiver receiver) {

        this.receiver = receiver;
    }

    @Override
    public CompletableFuture<Void> open() {

        // by convention the runtime is supposed to create a shared memory file at /temp/hazelcast-shm-<uniqueId>
        Path filepath = Paths.get(System.getProperty("java.io.tmpdir"), "hazelcast-shm-" + uniqueId);
        pipe = new SharedMemoryPipe(uniqueId, filepath, receiver, DATA_CAPACITY, SPIN_DELAY, OPEN_TIMEOUT);

        // shared memory pipe will try to connect for some time before giving up w/ UserCodeException
        pipe.open();

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> send(UserCodeMessage message) {

        pipe.write(message);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void destroy() {

        pipe.destroy();
    }
}

