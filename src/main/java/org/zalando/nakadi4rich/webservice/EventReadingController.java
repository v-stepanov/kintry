package org.zalando.nakadi4rich.webservice;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.zalando.nakadi4rich.service.ShardStream;
import org.zalando.nakadi4rich.model.view.NakadiBatch;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static spark.Spark.get;

public class EventReadingController {

    public static final int KINESIS_BATCH_SIZE = 100;
    public static final int KINESIS_WAIT_AFTER_NO_EVENTS = 100;

    private static AmazonKinesisClient client;
    private static ObjectMapper objectMapper;

    public static void create(final AmazonKinesisClient client, final ObjectMapper objectMapper) {
        EventReadingController.client = client;
        EventReadingController.objectMapper = objectMapper;
        handleGetEvents();
    }

    private static void handleGetEvents() {
        get("/event-types/:event-type/events", (req, res) -> {
            try {
                final List<Shard> shards;
                final String eventType = req.params(":event-type");
                try {
                    final DescribeStreamResult describeStreamResult = client.describeStream(eventType);
                    shards = describeStreamResult.getStreamDescription().getShards();
                } catch (ResourceNotFoundException e) {
                    res.status(404);
                    return "event type '" + eventType + "' not found";
                }

                final String nakadiCursors = req.headers("X-Nakadi-Cursors");
                final List<ShardStream> shardStreams = createShardStreams(shards, eventType, nakadiCursors);

                final HttpServletResponse httpResponse = res.raw();
                httpResponse.setHeader("transfer-encoding", "chunked");
                httpResponse.setStatus(200);
                final ServletOutputStream outputStream = httpResponse.getOutputStream();

                final int batchLimit = 2;
                final int batchTimeoutMs = 5000;

                while (true) {
                    try {
                        for (final ShardStream shardStream : shardStreams) {
                            NakadiBatch batchToSend = null;
                            final Optional<NakadiBatch> batchOrNone = shardStream.getFullBatch(batchLimit);

                            if (batchOrNone.isPresent()) {
                                batchToSend = batchOrNone.get();
                            } else if (currentTimeMillis() - shardStream.getLastFlushedAt() >= batchTimeoutMs) {
                                batchToSend = shardStream.getRemainingEvents();
                            }

                            if (batchToSend != null) {
                                writeData(outputStream, batchToSend);
                                shardStream.setLastFlushedAt(currentTimeMillis());
                            }
                        }
                    } catch (ProvisionedThroughputExceededException e) {
                        System.out.println("we are throttled");
                        Thread.sleep(1000);
                        return null;
                    }
                }
            } catch (IOException e) {
                System.out.println("user disconnected");
                return null;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("something went terribly wrong");
                return null;
            }
        });
    }

    private static List<ShardStream> createShardStreams(final List<Shard> shards, final String eventType,
                                                        final String nakadiCursors) throws IOException {

        if (nakadiCursors != null && !nakadiCursors.isEmpty()) {
            final List<NakadiBatch.Cursor> cursors =
                    objectMapper.readValue(nakadiCursors, new TypeReference<ArrayList<NakadiBatch.Cursor>>() {
                    });
            return cursors.stream()
                    .map(cursor -> {
                        final GetShardIteratorResult shardIteratorResult;
                        String lastOffset = cursor.getOffset();

                        if ("BEGIN".equals(cursor.getOffset())) {
                            final Shard shard = shards.stream()
                                    .filter(s -> s.getShardId().equals(cursor.getPartition()))
                                    .findFirst()
                                    .orElseThrow(() -> new RuntimeException("this should not happen!"));
                            final String oldestAvailable = shard.getSequenceNumberRange().getStartingSequenceNumber();
                            lastOffset = oldestAvailable;
                            shardIteratorResult = client.getShardIterator(eventType,
                                    cursor.getPartition(), ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),
                                    oldestAvailable);

                        } else if (ShardIteratorType.LATEST.toString().equals(cursor.getOffset())) {
                            shardIteratorResult = client.getShardIterator(eventType,
                                    cursor.getPartition(), ShardIteratorType.LATEST.toString());

                        } else {
                            shardIteratorResult = client.getShardIterator(eventType,
                                    cursor.getPartition(), ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                                    cursor.getOffset());
                        }
                        return new ShardStream(client, cursor.getPartition(), shardIteratorResult.getShardIterator(),
                                lastOffset);
                    })
                    .collect(Collectors.toList());

        } else {
            return shards.stream()
                    .map(shard -> {
                        final GetShardIteratorResult shardIteratorResult = client.getShardIterator(eventType,
                                shard.getShardId(), ShardIteratorType.LATEST.toString());
                        return new ShardStream(client, shard.getShardId(), shardIteratorResult.getShardIterator(),
                                ShardIteratorType.LATEST.toString());
                    })
                    .collect(Collectors.toList());
        }
    }

    private static void writeData(final ServletOutputStream outputStream, final NakadiBatch batch) throws IOException {
        outputStream.write(objectMapper.writeValueAsBytes(batch));
        outputStream.write("\n".getBytes(Charsets.UTF_8));
        outputStream.flush();
    }

}
