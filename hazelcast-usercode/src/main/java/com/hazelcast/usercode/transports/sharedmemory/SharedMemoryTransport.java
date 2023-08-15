package com.hazelcast.usercode.transports.sharedmemory;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.UserCodeException;
import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.UserCodeTransport;
import com.hazelcast.usercode.transports.MultiplexTransportBase;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// a UserCodeTransport that works over shared memory
public final class SharedMemoryTransport extends MultiplexTransportBase implements UserCodeTransportReceiver {

    // FIXME should be parameters & options
    private final int DATA_CAPACITY = 1024; // bytes
    private final int SPIN_DELAY = 4; // ms
    private final int OPEN_TIMEOUT = 5000; // ms

    private final UUID uniqueId;
    private final ILogger logger;
    private SharedMemoryPipe pipe;

    public SharedMemoryTransport(UUID uniqueId, LoggingService logging) {

        this.uniqueId = uniqueId;
        this.logger = logging.getLogger(SharedMemoryTransport.class);
    }

    @Override
    public CompletableFuture<Void> open() {

        // by convention the runtime is supposed to create a shared memory file at /temp/hazelcast-shm-<uniqueId>
        Path filepath = Paths.get(System.getProperty("java.io.tmpdir"), "hazelcast-shm-" + uniqueId);
        logger.info("Open SharedMemory transport (" + filepath + ")");
        pipe = new SharedMemoryPipe(uniqueId, filepath, this, DATA_CAPACITY, SPIN_DELAY, OPEN_TIMEOUT);

        // shared memory pipe will try to connect for some time before giving up w/ UserCodeException
        pipe.open();

        logger.info("Opened SharedMemory transport (" + filepath + ")");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<UserCodeMessage> invoke(UserCodeMessage message) {

        pipe.write(message);
        return createFuture(message);
    }

    @Override
    public void receive(UserCodeMessage message) {

        completeFuture(message);
    }

    @Override
    public void destroy() {

        pipe.destroy();
        failFutures(new UserCodeException("Transport closed."));
    }
}

