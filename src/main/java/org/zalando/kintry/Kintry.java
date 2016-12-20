package org.zalando.kintry;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;
import static java.lang.System.currentTimeMillis;
import static spark.Spark.get;
import static spark.Spark.post;

public class Kintry {

    private static AmazonKinesisClient kinesisClient;
    private static Random random;
    private static ObjectMapper objectMapper;

    static {
        random = new Random();
        kinesisClient = new AmazonKinesisClient();
        kinesisClient.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));
        objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    public static void main(String[] args) {
        get("/health", (req, res) -> {
            res.status(200);
            return "OK";
        });

        post("/events", (req, res) -> {
            try {
                final PutRecordRequest putRequest = new PutRecordRequest()
                        .withStreamName("kintry")
                        .withPartitionKey(String.valueOf(random.nextInt()))
                        .withData(ByteBuffer.wrap(req.bodyAsBytes()));

                final PutRecordResult result = kinesisClient.putRecord(putRequest);
                System.out.println(result.toString());

                res.status(201);
                return result.toString();

            } catch (Exception e) {
                e.printStackTrace();
                res.status(500);
                return e.getMessage();
            }
        });

        get("/stream", (req, res) -> {
            final HttpServletResponse raw = res.raw();
            raw.setHeader("transfer-encoding", "chunked");
            raw.setStatus(200);
            final ServletOutputStream outputStream = raw.getOutputStream();

            final String partition = "shardId-000000000001";
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                    .withStreamName("kintry")
                    .withShardId(partition)
                    .withShardIteratorType("TRIM_HORIZON");

            String shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest).getShardIterator();
            String lastOffset = "";

            final int batchTimeoutMs = 1000;
            long lastFlushedAt = currentTimeMillis();

            while (true) {
                try {
                    final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                            .withShardIterator(shardIterator)
                            .withLimit(1);

                    final GetRecordsResult recordsResult = kinesisClient.getRecords(recordsRequest);

                    System.out.println("got " + recordsResult.getRecords().size() + " records");
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
                    }
                    else {
                        if (currentTimeMillis() - lastFlushedAt >= batchTimeoutMs) {
                            writeData(outputStream, partition, lastOffset, ImmutableList.of());
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
                }
            }
        });
    }

    private static void writeData(final ServletOutputStream outputStream, final String partition,
                                  final String lastOffset, final List<String> events) throws IOException {
        final NakadiBatch.Cursor cursor = new NakadiBatch.Cursor(partition, lastOffset);
        final NakadiBatch nakadiBatch = new NakadiBatch(cursor, events);
        outputStream.write(objectMapper.writeValueAsBytes(nakadiBatch));
        outputStream.write("\n".getBytes(Charsets.UTF_8));
        outputStream.flush();
    }
}
