package com.hazelcast.jet;

import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;

public class Utils {

    private static ForkJoinPool forkJoinPool;

    public static ManagedChannelBuilder<?> createChannelBuilder(String host, int port, String executor) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port)
                                                         .usePlaintext();

        if ("fork-join".equals(executor)) {
            builder.executor(getForkJoinPool());
        }
        if ("fixed-thread".equals(executor)) {
            builder.executor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
        }
        if ("direct".equals(executor)) {
            builder.directExecutor();
        }
        return builder;
    }

    private static synchronized ForkJoinPool getForkJoinPool() {
        if (forkJoinPool == null) {
            forkJoinPool = new ForkJoinPool(
                    Runtime.getRuntime().availableProcessors(),
                    new ForkJoinWorkerThreadFactory() {
                        final AtomicInteger num = new AtomicInteger();

                        @Override
                        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                            ForkJoinWorkerThread thread = defaultForkJoinWorkerThreadFactory.newThread(pool);
                            thread.setDaemon(true);
                            thread.setName("grpc-client-app-" + "-" + num.getAndIncrement());
                            return thread;
                        }
                    }, UncaughtExceptionHandlers.systemExit(), true /* async */);
        }
        return forkJoinPool;
    }

    public static String getProp(String name) {
        String property = System.getProperty(name);
        if (property == null) {
            throw new RuntimeException("Could not find property " + name);
        }
        return property;
    }

    public static int getIntProp(String name) {
        String value = getProp(name);
        return Integer.parseInt(value);
    }

    public static int getIntProp(String name, String def) {
        String value = System.getProperty(name, def);
        return Integer.parseInt(value);
    }

    public static int[] getIntPropArray(String name, String def) {
        String value = System.getProperty(name, def);
        String[] parts = value.split(",");
        int[] array = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            array[i] = Integer.parseInt(parts[i]);
        }
        return array;
    }
}
