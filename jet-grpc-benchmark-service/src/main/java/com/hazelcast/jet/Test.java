package com.hazelcast.jet;

import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc.GreeterStub;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Test {

    public static final int THREADS = 48;

    public static void main(String[] args) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(THREADS);
        long start = System.currentTimeMillis();
        for (int i = 0; i < THREADS; i++) {

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 42123)
                                                          .directExecutor()
                                                          .usePlaintext()
                                                          .build();
            GreeterStub stub = GreeterGrpc.newStub(channel);
            for (int k = 0; k < 1; k++) {

                new Thread(() -> {

                    for (int j = 0; j < 100_000; j++) {
                        HelloRequest request = HelloRequest.newBuilder().setValue(j).build();
                        CompletableFuture<Integer> future = new CompletableFuture<>();
                        stub.sayHelloUnary(request, new StreamObserver<HelloReply>() {
                            @Override
                            public void onNext(HelloReply value) {
                                future.complete(value.getValue());
                            }

                            @Override
                            public void onError(Throwable t) {
                                t.printStackTrace();
                            }

                            @Override
                            public void onCompleted() {
                                if (!future.isDone()) {
                                    future.completeExceptionally(new RuntimeException("completed when not done"));
                                }
                            }
                        });
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    latch.countDown();
                }).start();
            }
        }
        latch.await();
        long time = System.currentTimeMillis() - start;
        System.out.println("Took: " + time + " avg/s " + (THREADS * 100_000) / (time / 1000));
    }
}
