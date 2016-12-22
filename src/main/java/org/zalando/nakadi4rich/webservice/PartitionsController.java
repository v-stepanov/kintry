package org.zalando.nakadi4rich.webservice;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi4rich.model.view.Partition;

import java.util.List;
import java.util.stream.Collectors;

import static spark.Spark.get;

public class PartitionsController {

    private static AmazonKinesisClient client;
    private static ObjectMapper objectMapper;

    public static void create(final AmazonKinesisClient client, final ObjectMapper objectMapper) {
        PartitionsController.client = client;
        PartitionsController.objectMapper = objectMapper;
        handleGetPartitions();
    }

    private static void handleGetPartitions() {
        get("/event-types/:event-type/partitions", (req, res) -> {
            final String eventType = req.params(":event-type");
            final List<Shard> shards;
            try {
                final DescribeStreamResult describeStreamResult = client.describeStream(eventType);
                shards = describeStreamResult.getStreamDescription().getShards();
            } catch (ResourceNotFoundException e) {
                res.status(404);
                return "event type '" + eventType + "' not found";
            }
            final List<Partition> partitions = shards.stream()
                    .map(shard -> new Partition(shard.getShardId(),
                            shard.getSequenceNumberRange().getStartingSequenceNumber(),
                            ShardIteratorType.LATEST.toString()))
                    .collect(Collectors.toList());

            res.status(200);
            return objectMapper.writeValueAsString(partitions);
        });
    }

}
