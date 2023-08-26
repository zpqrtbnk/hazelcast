package com.hazelcast.usercode.services;

import java.util.concurrent.*;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.usercode.UserCodeException;
import com.hazelcast.usercode.grpc.controller.*;
import com.hazelcast.usercode.grpc.usercode.TransportGrpc;
import com.hazelcast.usercode.runtimes.UserCodeContainerRuntime;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;

public final class UserCodeRuntimeController {

    private static final int DEADLINE_FOR_INVOCATION = 10; // seconds
    private static final int USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_SECONDS = 1800; // seconds
    private static final int USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_TIMEOUT_SECONDS = 20; // seconds
    private final ILogger logger;
    private ManagedChannel channel;
    ControllerGrpc.ControllerFutureStub futureStub;
    //ControllerGrpc.ControllerBlockingStub blockingStub;

    public UserCodeRuntimeController(String address, int port, ILogger logger) {
        this.logger = logger;

        this.channel = ManagedChannelBuilder
                .forAddress(address, port)
                .keepAliveTime(USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS) // TODO make it an option
                .keepAliveTimeout(USERCODERUNTIME_CONTROLLER_KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS) // TODO same
                .keepAliveWithoutCalls(true)
                .enableRetry()
                .usePlaintext()
                .build();

        futureStub = ControllerGrpc.newFutureStub(channel);
        //blockingStub = ControllerGrpc.newBlockingStub(channel);
    }

    public CompletableFuture<String> createContainer(String image) {

        CreateRequest request = CreateRequest.newBuilder()
                .setImage(image)
                .build();

        int timeoutSeconds = 10;

        ListenableFuture<CreateResponse> future =
            (timeoutSeconds > 0
                ? futureStub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                : futureStub
            )
            .create(request);

        // FIXME handle exceptions in the future
        //logger.severe("Failed to start a new user code container runtime.", e);
        //throw new UserCodeException("Failed to start a new user code container runtime.", e);
        return completable(future).thenApply(CreateResponse::getName);
    }

    public CompletableFuture<Void> deleteContainer(String name) {
        // If we observe the output and act according to result then what thread will
        // be responsible to manage or retry that request since it is an async call.
        // ??

        DeleteRequest request = DeleteRequest
                .newBuilder()
                .setName(name)
                .build();

        ListenableFuture<Empty> future = futureStub
                .withDeadlineAfter(DEADLINE_FOR_INVOCATION, TimeUnit.SECONDS)
                .withWaitForReady()
                .delete(request);

        return completable(future).thenApply(x -> null);
    }

    // grpc produces ListenableFuture and we want CompletableFuture and Java really is meh
    private <T> CompletableFuture<T> completable(ListenableFuture<T> listenable) {

        CompletableFuture<T> completable = new CompletableFuture<>();
        ExecutorService executor = Executors.newFixedThreadPool(10); // FIXME this is positively UGLY

        Futures.addCallback(listenable, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        }, executor);

        return completable;
    }
}
