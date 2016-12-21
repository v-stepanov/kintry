package org.zalando.kintry;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.base.Charsets;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.System.currentTimeMillis;

class ShardStream {

    private long lastFlushedAt;
    private long nextFetch;
    private String iterator;
    private String lastOffset;

    private final String shardId;
    private final String eventType;
    private final LinkedList<Record> events;
    private final AmazonKinesisClient client;

    public ShardStream(final AmazonKinesisClient client, final String eventType, final String shardId,
                       final String iterator, final String lastOffset) {
        this.client = client;
        this.eventType = eventType;
        this.iterator = iterator;
        this.shardId = shardId;
        this.events = new LinkedList<>();
        this.lastFlushedAt = currentTimeMillis();
        this.nextFetch = currentTimeMillis();
        this.lastOffset = lastOffset;
    }

    public Optional<NakadiBatch> getFullBatch(final int amount) {
        if (events.size() < amount) {
            readFromKinesis();
        }
        if (events.size() < amount) {
            return Optional.empty();
        } else {
            final List<Record> records = IntStream.range(0, amount)
                    .mapToObj(x -> events.poll())
                    .collect(Collectors.toList());
            final Record lastRecord = records.get(records.size() - 1);
            lastOffset = lastRecord.getSequenceNumber();

            final List<String> batchEvents = records.stream()
                    .map(record -> new String(record.getData().array(), Charsets.UTF_8))
                    .collect(Collectors.toList());
            return Optional.of(new NakadiBatch(shardId, lastRecord.getSequenceNumber(), batchEvents));
        }
    }

    public NakadiBatch getRemainingEvents() {
        String offset;
        try {
            offset = events.getLast().getSequenceNumber();
            lastOffset = offset;
        } catch (NoSuchElementException e) {
            offset = lastOffset;
        }
        final List<String> batchEvents = events.stream()
                .map(record -> new String(record.getData().array(), Charsets.UTF_8))
                .collect(Collectors.toList());
        events.clear();
        return new NakadiBatch(shardId, offset, batchEvents);
    }

    public void readFromKinesis() {
        if (currentTimeMillis() < nextFetch) {
            return;
        }
        final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                .withShardIterator(iterator)
                .withLimit(EventReadingController.KINESIS_BATCH_SIZE);

        final GetRecordsResult recordsResult = client.getRecords(recordsRequest);
        iterator = recordsResult.getNextShardIterator();

        final List<Record> records = recordsResult.getRecords();
        if (records.isEmpty()) {
            nextFetch = currentTimeMillis() + EventReadingController.KINESIS_WAIT_AFTER_NO_EVENTS;
        } else {
            events.addAll(records);
        }
    }

    public long getLastFlushedAt() {
        return lastFlushedAt;
    }

    public void setLastFlushedAt(final long lastFlushedAt) {
        this.lastFlushedAt = lastFlushedAt;
    }

}
