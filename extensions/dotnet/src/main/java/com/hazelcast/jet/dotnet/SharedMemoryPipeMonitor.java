package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class SharedMemoryPipeMonitor {

    private final Thread thread;
    private final SharedMemoryPipe pipe;
    private boolean running;
    private int spinDelay;
    private CompletableFuture<Void> readFuture;
    private CompletableFuture<Void> writeFuture;
    private int requiredBytes;

    // https://jenkov.com/tutorials/java-concurrency/creating-and-starting-threads.html

    private class MonitorThread extends Thread {

        public MonitorThread() {
            super("hz-shmpipe-" + pipe.getUid());
        }

        public void run() {
            while (running) {
                if (readFuture != null && pipe.canRead()) {
                    CompletableFuture<Void> f = readFuture;
                    readFuture = null;
                    f.complete(null);
                }
                if (writeFuture != null && pipe.canWrite(requiredBytes)) {
                    CompletableFuture<Void> f = writeFuture;
                    writeFuture = null;
                    requiredBytes = 0;
                    f.complete(null);
                }

                try {
                    // FIXME we can do better
                    //   as long as we don't have futures, we don't need to spin
                    if (spinDelay > 0)
                        TimeUnit.MILLISECONDS.sleep(spinDelay);
                }
                catch (InterruptedException ie) {
                    // FIXME this is a bad idea
                }
            }
        }
    }

    // initializes a new monitor
    public SharedMemoryPipeMonitor(SharedMemoryPipe pipe, int spinDelay) {

        this.pipe = pipe;
        this.spinDelay = spinDelay;
        this.running = true;

        thread = new MonitorThread();
        thread.start();
    }

    // NOTE: the pipe monitor (and the pipe itself) is NOT thread-safe at
    // the moment, it's only one read or one write and one at a time, so
    // waitCanRead and waitCanWrite assume that if they have to wait and
    // return a future, there is NOT another future

    public CompletableFuture<Void> waitCanRead() {

        if (pipe.canRead()) return CompletableFuture.completedFuture(null);
        return readFuture = new CompletableFuture<>();
    }

    public CompletableFuture<Void> waitCanWrite(int requiredBytes) {

        this.requiredBytes = Math.max(this.requiredBytes, requiredBytes);
        if (pipe.canWrite(requiredBytes)) return CompletableFuture.completedFuture(null);
        return writeFuture = new CompletableFuture<>();
    }

    public void destroy() {

        running = false;
        try {
            thread.join();
        }
        catch (InterruptedException e) {
            // FIXME this is a bad idea
        }
    }
}
