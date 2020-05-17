package com.hazelcast.jet;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReplyList;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequestList;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Utils.createChannelBuilder;
import static com.hazelcast.jet.Utils.getIntProp;
import static com.hazelcast.jet.Utils.getProp;
import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.averagingLong;

public class StreamBenchmarkJob {

    private final JetInstance jet;
    private final Pipeline pipeline;

    private final String runId;
    private final int windowSizeSeconds;

    public StreamBenchmarkJob(JetInstance jet, String runId, Pipeline pipeline, int windowSizeSeconds) {
        this.runId = runId;
        this.jet = jet;
        this.pipeline = pipeline;
        this.windowSizeSeconds = windowSizeSeconds;
    }

    public String runAndReportStream() {
        JobConfig config = new JobConfig();
        config.addClass(StreamBenchmarkJob.class);
        config.addPackage("com.hazelcast.jet.grpc.greeter");

        Job job = jet.newJob(pipeline, config);

        Observable<Long> observable = jet.getObservable(runId);

        CompletableFuture<Double> future =
                observable.toFuture(r -> r.skip(2).limit(5).collect(averagingLong(e -> e)));

        try {
            Double avg = future.get();
            return String.format(runId + ": avg/s %.2f", avg / (windowSizeSeconds * 5));

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Could not finish benchmark", e);
        } finally {
            job.cancel();
            observable.destroy();
        }
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        String runId = UuidUtil.newUnsecureUuidString();

        String host = getProp("host");
        int port = getIntProp("port");
        int maxItemsPerSecond = getIntProp("maxItemsPerSecond", "10000000");
        int windowSizeSeconds = getIntProp("windowSize", "30");
        long windowSizeMillis = Duration.ofSeconds(windowSizeSeconds).toMillis();
        int mapBatchSize = getIntProp("mapBatchSize", "1024");
        int maxConcurrentOps = getIntProp("maxConcurrentOps", "4");
        int localParallelism = getIntProp("localParallelism", String.valueOf(Runtime.getRuntime().availableProcessors()));

        List<String> results = synchronizedList(new ArrayList<>());


        String streamUnaryRunId = runId + "-stream-unary";
        Pipeline streamUnaryPipeline = unary(streamUnaryRunId, host, port, maxItemsPerSecond, windowSizeMillis, maxConcurrentOps);
        results.add(new StreamBenchmarkJob(jet, streamUnaryRunId, streamUnaryPipeline, windowSizeSeconds).runAndReportStream());

        String streamUnaryBatchRunId = runId + "-stream-unary-batch";
        Pipeline unaryBatchPipeline = unaryBatch(streamUnaryBatchRunId, host, port, maxItemsPerSecond, mapBatchSize, windowSizeMillis);
        results.add(new StreamBenchmarkJob(jet, streamUnaryBatchRunId, unaryBatchPipeline, windowSizeSeconds).runAndReportStream());

        String streamBidiRunId = runId + "-stream-bidi";
        Pipeline bidiPipeline = bidirectinalStreaming(streamBidiRunId, host, port, maxItemsPerSecond, windowSizeMillis, maxConcurrentOps);
        results.add(new StreamBenchmarkJob(jet, streamBidiRunId, bidiPipeline, windowSizeSeconds).runAndReportStream());

        String streamBidiBatchRunId = runId + "-stream-bidi-batch";
        Pipeline p = bidirectionalStreamingBatch(streamBidiBatchRunId, host, port, maxItemsPerSecond, mapBatchSize, windowSizeMillis);
        results.add(new StreamBenchmarkJob(jet, streamBidiBatchRunId, p, windowSizeSeconds).runAndReportStream());

        jet.shutdown();

        try (FileOutputStream out = new FileOutputStream("results-" + runId + ".txt"); PrintWriter writer = new PrintWriter(out)) {

            writer.println("maxItemsPerSecond=" + maxItemsPerSecond);
            writer.println("windowSize=" + windowSizeMillis);
            writer.println("mapBatchSize=" + mapBatchSize);
            writer.println("maxConcurrentOps=" + maxConcurrentOps);
            writer.println("localParallelism=" + localParallelism);

            for (String result : results) {
                System.out.println(result);
                writer.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Pipeline unary(String runId, String host, int port, int maxItemsPerSecond, long windowSize,
                                 int maxConcurrentOps) {
        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> unaryService = unaryService(
                () -> createChannelBuilder(host, port, "executor"),
                channel -> GreeterGrpc.newStub(channel)::sayHelloUnary
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(maxItemsPerSecond))
         .withIngestionTimestamps()
         .map(event -> (int) event.sequence())
         .mapUsingServiceAsync(unaryService, maxConcurrentOps, true,
                 (service, item) -> service.call(HelloRequest.newBuilder().setValue(item).build()))
         .window(WindowDefinition.tumbling(windowSize))
         .aggregate(AggregateOperations.counting())
         .map(WindowResult::result)
         .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline unaryBatch(String runId, String host, int port, int itemsPerSecond, int batchSize,
                                      long windowSize) {
        ServiceFactory<?, ? extends GrpcService<HelloRequestList, HelloReplyList>> unaryService = unaryService(
                () -> createChannelBuilder(host, port, "executor"),
                channel -> GreeterGrpc.newStub(channel)::sayHelloListUnary
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(itemsPerSecond))
         .withIngestionTimestamps()
         .map(event -> (int) event.sequence())
         .mapUsingServiceAsyncBatched(unaryService, batchSize,
                 (service, items) -> service.call(HelloRequestList.newBuilder().addAllValue(items).build())
                                            .thenApply(HelloReplyList::getValueList)
         )
         .window(WindowDefinition.tumbling(windowSize))
         .aggregate(AggregateOperations.counting())
         .map(WindowResult::result)
         .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline bidirectinalStreaming(String runId, String host, int port, int itemsPerSecond, long windowSize, int maxConcurrentOps) {
        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> bidiService = bidirectionalStreamingService(
                () -> createChannelBuilder(host, port, "executor"),
                channel -> GreeterGrpc.newStub(channel)::sayHelloBidirectional
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(itemsPerSecond))
         .withIngestionTimestamps()
         .map(event -> (int) event.sequence())
         .mapUsingServiceAsync(bidiService, maxConcurrentOps, true,
                 (service, item) -> service.call(HelloRequest.newBuilder().setValue(item).build()))
         .window(WindowDefinition.tumbling(windowSize))
         .aggregate(AggregateOperations.counting())
         .map(WindowResult::result)
         .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline bidirectionalStreamingBatch(String runId, String host, int port, int itemsPerSecond,
                                                       int batchSize, long windowSize) {
        ServiceFactory<?, ? extends GrpcService<HelloRequestList, HelloReplyList>> bidiService =
                bidirectionalStreamingService(
                        () -> createChannelBuilder(host, port, "executor"),
                        channel -> GreeterGrpc.newStub(channel)::sayHelloListBidirectional
                );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(itemsPerSecond))
         .withIngestionTimestamps()
         .map(simpleEvent -> (int) simpleEvent.sequence())
         .mapUsingServiceAsyncBatched(bidiService,
                 batchSize,
                 (service, itemList) -> {
                     CompletableFuture<HelloReplyList> future =
                             service.call(HelloRequestList.newBuilder().addAllValue(itemList).build());
                     return future.thenApply(HelloReplyList::getValueList);
                 })
         .window(WindowDefinition.tumbling(windowSize))
         .aggregate(AggregateOperations.counting())
         .map(WindowResult::result)
         .writeTo(Sinks.observable(runId));

        return p;
    }
}
