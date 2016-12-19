package org.zalando.kintry;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;
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

        get("/events", (req, res) -> {
            try {
                GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                        .withStreamName("kintry")
                        .withShardId("shardId-000000000001")
                        .withShardIteratorType("TRIM_HORIZON");

                final String shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest).getShardIterator();

                final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                        .withShardIterator(shardIterator)
                        .withLimit(100);

                final GetRecordsResult recordsResult = kinesisClient.getRecords(recordsRequest);

                res.status(200);
                return recordsResult.getRecords().stream()
                        .map(r -> new String(r.getData().array()) + " " + r.getPartitionKey() + "\n" + r.getSequenceNumber())
                        .collect(Collectors.joining("\n"));
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500);
                return "";
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

            while (true) {
                try {
                    final GetRecordsRequest recordsRequest = new GetRecordsRequest()
                            .withShardIterator(shardIterator)
                            .withLimit(4);

                    final GetRecordsResult recordsResult = kinesisClient.getRecords(recordsRequest);
                    shardIterator = recordsResult.getNextShardIterator();

                    final List<Record> records = recordsResult.getRecords();
                    final List<String> events = records.stream()
                            .map(r -> new String(r.getData().array(), Charsets.UTF_8))
                            .collect(Collectors.toList());

                    if (!events.isEmpty()) {
                        final Record lastEvent = records.get(records.size() - 1);
                        lastOffset = lastEvent.getSequenceNumber();
                    }

                    final NakadiBatch.Cursor cursor = new NakadiBatch.Cursor(partition, lastOffset);
                    final NakadiBatch nakadiBatch = new NakadiBatch(cursor, events);

                    outputStream.write(objectMapper.writeValueAsBytes(nakadiBatch));
                    outputStream.write("\n".getBytes(Charsets.UTF_8));
                    outputStream.flush();
                    Thread.sleep(1);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    return null;
                }
            }

        });
    }
}
