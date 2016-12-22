package org.zalando.kintry.webservice;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import java.nio.ByteBuffer;
import java.util.Random;

import static spark.Spark.post;

public class EventPublishingController {

    private static AmazonKinesisClient client;
    private static Random random = new Random();

    public static void create(final AmazonKinesisClient client) {
        EventPublishingController.client = client;
        handlePostEvents();
    }

    private static void handlePostEvents() {
        post("/event-types/:event-type/events", (req, res) -> {

            final String eventType = req.params(":event-type");
            try {
                final PutRecordRequest putRequest = new PutRecordRequest()
                        .withStreamName(eventType)
                        .withPartitionKey(String.valueOf(random.nextInt()))
                        .withData(ByteBuffer.wrap(req.bodyAsBytes()));

                final PutRecordResult result = client.putRecord(putRequest);
                System.out.println(result.toString());

                res.status(201);
                return "";

            } catch (ResourceNotFoundException e) {
                res.status(404);
                return "event type '" + eventType + "' not found";
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500);
                return e.getMessage();
            }
        });
    }

}
