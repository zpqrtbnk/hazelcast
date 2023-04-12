package com.hazelcast.jet.dotnet;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class ShmPipeMonitor {

    private final Thread thread;
    private final ShmPipe pipe;
    private boolean running;
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

                /**/
                try {
                    // FIXME what's a good value? when going REALLY fast...
                    // in fact, setting a future should trigger an immediate verification!
                    // and, we should stop spinning (and do?) if we don't have a future
                    // blocking the thread with some sort of semaphore
                    if (pipe.getSpinDelay() > 0)
                        TimeUnit.MILLISECONDS.sleep(pipe.getSpinDelay());
                }
                catch (InterruptedException ie) {
                    // ??
                }
            }
        }
    }

    // initializes a new monitor
    public ShmPipeMonitor(ShmPipe pipe) {

        this.pipe = pipe;
        this.running = true;

        thread = new MonitorThread();
        thread.start();
    }

    public CompletableFuture<Void> waitCanRead() {

        if (readFuture != null) return readFuture; // FIXME race! + should NEVER happen
        if (pipe.canRead()) return CompletableFuture.completedFuture(null);
        return readFuture = new CompletableFuture<>();
    }

    public CompletableFuture<Void> waitCanWrite(int requiredBytes) {

        this.requiredBytes = Math.max(this.requiredBytes, requiredBytes);
        if (writeFuture != null) return writeFuture; // FIXME race! + should NEVER happen
        if (pipe.canWrite(requiredBytes)) return CompletableFuture.completedFuture(null);
        return writeFuture = new CompletableFuture<>();
    }

    public void destroy() {

        running = false;
        try {
            thread.join();
        }
        catch (InterruptedException e) {
            // FIXME meh?
        }
    }
}
