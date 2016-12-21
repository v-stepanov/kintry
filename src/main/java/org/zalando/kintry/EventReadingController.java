package org.zalando.kintry;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import spark.Route;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        get("/event-types/:event-type/events", handleReading());
    }


    private static Route handleReading() {
        return (req, res) -> {

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
            final Map<String, String> iterators = getInitialIterators(shards, eventType, nakadiCursors);

            final HttpServletResponse httpResponse = res.raw();
            httpResponse.setHeader("transfer-encoding", "chunked");
            httpResponse.setStatus(200);
            final ServletOutputStream outputStream = httpResponse.getOutputStream();

            String lastOffset = "";

            final int batchLimit = 10;
            final int batchTimeoutMs = 1000;
//            long lastFlushedAt = currentTimeMillis();

            final Map<String, Long> lastFlushedAt = iterators.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> currentTimeMillis()));

            while (true) {
                /*try {
                    for (final Map.Entry<String, String> iteratorForShard : iterators.entrySet()) {
                        final String shardId = iteratorForShard.getKey();
                        final String iterator = iteratorForShard.getValue();

                        final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                                .withShardIterator(iterator)
                                .withLimit(batchLimit);

                        final GetRecordsResult recordsResult = client.getRecords(recordsRequest);
                        iteratorForShard.setValue(recordsResult.getNextShardIterator());

                        final List<Record> records = recordsResult.getRecords();
                        final List<String> events = records.stream()
                                .map(r -> new String(r.getData().array(), Charsets.UTF_8))
                                .collect(Collectors.toList());
                    }
                    final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                            .withShardIterator(shardIterator)
                            .withLimit(batchLimit);

                    final GetRecordsResult recordsResult = client.getRecords(recordsRequest);
                    shardIterator = recordsResult.getNextShardIterator();

                    final List<Record> records = recordsResult.getRecords();
                    final List<String> events = records.stream()
                            .map(r -> new String(r.getData().array(), Charsets.UTF_8))
                            .collect(Collectors.toList());

                    if (!events.isEmpty()) {
                        final Record lastEvent = records.get(records.size() - 1);
                        lastOffset = lastEvent.getSequenceNumber();
                        writeData(outputStream, partition, lastOffset, events);
                        lastFlushedAt = currentTimeMillis();
                    } else {
                        if (currentTimeMillis() - lastFlushedAt >= batchTimeoutMs) {
                            if (!events.isEmpty()) {
                                final Record lastEvent = records.get(records.size() - 1);
                                lastOffset = lastEvent.getSequenceNumber();
                            }
                            writeData(outputStream, partition, lastOffset, events);
                            lastFlushedAt = currentTimeMillis();
                        }
                        Thread.sleep(10);
                    }
                } catch (ProvisionedThroughputExceededException e) {
                    System.out.println("we are throttled");
                    Thread.sleep(1000);
                    return null;
                } catch (IOException e) {
                    System.out.println("user disconnected");
                    return null;
                } catch (Exception e) {
                    System.out.println("something went terribly wrong");
                    return null;
                }*/
            }
        };
    }

    private static Map<String, String> getInitialIterators(final List<Shard> shards, final String eventType,
                                                           final String nakadiCursors) throws IOException {
        final Map<String, String> iterators;
        if (nakadiCursors != null && nakadiCursors.isEmpty()) {
            final List<NakadiBatch.Cursor> cursors =
                    objectMapper.readValue(nakadiCursors, new TypeReference<ArrayList<NakadiBatch.Cursor>>() {
                    });
            iterators = cursors.stream()
                    .collect(Collectors.toMap(
                            NakadiBatch.Cursor::getPartition,
                            cursor -> client.getShardIterator(eventType, cursor.getPartition(),
                                    ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString()).getShardIterator()));

        } else {
            iterators = shards.stream()
                    .collect(Collectors.toMap(
                            Shard::getShardId,
                            shard -> client.getShardIterator(eventType, shard.getShardId(),
                                    ShardIteratorType.LATEST.toString()).getShardIterator()));
        }
        return iterators;
    }

    private static void writeData(final ServletOutputStream outputStream, final String partition,
                                  final String lastOffset, final List<String> events) throws IOException {
        final NakadiBatch.Cursor cursor = new NakadiBatch.Cursor(partition, lastOffset);
        final NakadiBatch nakadiBatch = new NakadiBatch(cursor, events);
        outputStream.write(objectMapper.writeValueAsBytes(nakadiBatch));
        outputStream.write("\n".getBytes(Charsets.UTF_8));
        outputStream.flush();
    }

    private static class ShardStream {

        private long lastFlushedAt;
        private long nextFetch;
        private String iterator;
        private final String shardId;
        private final LinkedList<String> events;
        private final AmazonKinesisClient client;

        public ShardStream(final String shardId, final String iterator, final AmazonKinesisClient client) {
            this.iterator = iterator;
            this.shardId = shardId;
            this.client = client;
            this.events = new LinkedList<>();
            lastFlushedAt = currentTimeMillis();
            nextFetch = currentTimeMillis();
        }

        public Optional<List<String>> retrieveAmountOrNone(final int amount) {
            if (events.size() < amount) {
                return Optional.empty();
            } else {
                return Optional.of(
                        IntStream.range(0, amount)
                                .mapToObj(x -> events.poll())
                                .collect(Collectors.toList()));
            }
        }

        public List<String> retrieveAmountOrLess(final int amount) {
            return null;
        }

        private void readFromKinesis() {
            if (currentTimeMillis() < nextFetch) {
                return;
            }
            final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                    .withShardIterator(iterator)
                    .withLimit(KINESIS_BATCH_SIZE);

            final GetRecordsResult recordsResult = client.getRecords(recordsRequest);
            iterator = recordsResult.getNextShardIterator();

            final List<Record> records = recordsResult.getRecords();
            if (records.isEmpty()) {
                nextFetch = currentTimeMillis() + KINESIS_WAIT_AFTER_NO_EVENTS;
            } else {
                records.stream()
                        .map(record -> new String(record.getData().array(), Charsets.UTF_8))
                        .forEach(this.events::add);
            }
        }
    }
}
