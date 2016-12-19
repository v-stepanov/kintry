package org.zalando.kintry;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;

import static spark.Spark.get;
import static spark.Spark.post;

public class Kintry {

    private static AmazonKinesisClient kinesisClient;
    private static Random random;

    static {
        random = new Random();
        kinesisClient = new AmazonKinesisClient();
        kinesisClient.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));
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

            while (true) {
                try {
                    outputStream.write("blah\n".getBytes());
                    outputStream.flush();
                    Thread.sleep(1000);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    return null;
                }
            }

        });
    }
}
