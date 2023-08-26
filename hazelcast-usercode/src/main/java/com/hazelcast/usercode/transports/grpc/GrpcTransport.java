package com.hazelcast.usercode.transports.grpc;

import com.google.protobuf.ByteString;
import com.hazelcast.jet.jobbuilder.InfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.usercode.UserCodeException;
import com.hazelcast.usercode.grpc.usercode.*;

import com.hazelcast.usercode.UserCodeMessage;
import com.hazelcast.usercode.transports.MultiplexTransportBase;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// a UserCodeTransport that works over gRPC
public final class GrpcTransport extends MultiplexTransportBase {

    private final String address;
    private final int port;
    private final ILogger logger;
    private ManagedChannel channel;
    private StreamObserver<UserCodeGrpcMessage> grpc;
    private CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile Throwable exceptionInOutputObserver; // TODO: take care of it, of server's health (see python)

    public GrpcTransport(InfoMap transportInfo, LoggingService logging) {

        String address = transportInfo.childAsString("address", "localhost");
        int port = transportInfo.childAsInteger("port", 80);

        this.address = address;
        this.port = port;
        this.logger = logging.getLogger(GrpcTransport.class);
    }

    @Override
    public CompletableFuture<Void> open() {

        logger.info("Open gRPC transport (" + address + ":" + port + ")");

        int timeout = 30000; // ms
        int elapsed = 0;
        while (!serverListening(address, port)) {
            if (elapsed > timeout) {
                throw new UserCodeException("Failed to detect listening gRPC server.");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(500);
                elapsed += 500;
            }
            catch (InterruptedException ex) {
                return null; // FIXME?
            }
        }

        logger.info("Detected gRPC server");

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forAddress(address, port)
                //.intercept(new TimeoutInterceptor(timeoutValue, timeoutUnit))
                .usePlaintext();
        channel = channelBuilder.build(); // TODO: why is the original python thing using netty?
        TransportGrpc.TransportStub stub = TransportGrpc.newStub(channel);

        //stub = stub.withDeadlineAfter(4, TimeUnit.SECONDS); // deadline not a solution to streaming calls...
        // see https://github.com/grpc/grpc-java/issues/5498
        // better do something with timers? but then ... it should be done at futures level

        grpc = stub.invoke(new MessageReceiver());
        // TODO: consider pinging / validating that we are connected?
        logger.info("Opened Grpc transport (" + address + ":" + port + ")");
        return CompletableFuture.completedFuture(null);
    }

    public static boolean serverListening(String host, int port)
    {
        Socket s = null;
        try
        {
            s = new Socket(host, port);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
        finally
        {
            if(s != null)
                try { s.close(); }
                catch (Exception e) { }
        }
    }

    private class MessageReceiver implements StreamObserver<UserCodeGrpcMessage> {

        @Override
        public void onNext(UserCodeGrpcMessage value) {

            logger.info("Transport received message");

            try {

                long id = value.getId();
                String functionName = value.getFunctionName();
                byte[] payload = value.getPayload().toByteArray(); // copies bytes
                //ByteBuffer payload = value.getPayload().asReadOnlyByteBuffer(); // would be better
                UserCodeMessage message = new UserCodeMessage(id, functionName, payload);
                completeFuture(message);
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {

            logger.severe("Transport error", e);

            try {
                Status status = Status.fromThrowable(e);
                e = GrpcUtil.translateGrpcException(e);
                exceptionInOutputObserver = e;
                failFutures(e);
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            logger.info("Transport completed");
            failFutures(new UserCodeException("Completion of transport signaled before the future was completed."));
            completionLatch.countDown();
        }
    }

    @Override
    public CompletableFuture<UserCodeMessage> invoke(UserCodeMessage message) {

        UserCodeGrpcMessage input = UserCodeGrpcMessage.newBuilder()
                .setId(message.getId())
                .setFunctionName(message.getFunctionName())
                //.setPayload(new LiteralByteString(message.getPayload())) // zero-copy?!
                .setPayload(ByteString.copyFrom(message.getPayload())) // copy :(
                .build();

        CompletableFuture<UserCodeMessage> future = createFuture(message);

        try{
            logger.info("Transport sending message");
            grpc.onNext(input);
            logger.info("Transport sent (??) message");
        }
        catch (Exception ex) {
            grpc.onError(ex);
            logger.severe("meh", ex);
            failFuture(message, ex);
            throw ex;
        }

        return future;
    }

    @Override
    public void destroy() {
        grpc.onCompleted();
        try {
            completionLatch.await(4, TimeUnit.SECONDS); // TODO: extract constant
            GrpcUtil.shutdownChannel(channel, null, 1); // TODO: extract timeout constant
        }
        catch (InterruptedException ex) {
            // ok now what?
            // this is bad
        }
        failFutures(new UserCodeException("Transport closed."));
    }
}
