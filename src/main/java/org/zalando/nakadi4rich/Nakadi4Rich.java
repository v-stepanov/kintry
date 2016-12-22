package org.zalando.nakadi4rich;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi4rich.webservice.EventPublishingController;
import org.zalando.nakadi4rich.webservice.EventReadingController;
import org.zalando.nakadi4rich.webservice.PartitionsController;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;
import static spark.Spark.get;

public class Nakadi4Rich {

    public static void main(String[] args) {
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient();
        kinesisClient.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));

        final ObjectMapper objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        EventPublishingController.create(kinesisClient);
        EventReadingController.create(kinesisClient, objectMapper);
        PartitionsController.create(kinesisClient, objectMapper);

        get("/health", (req, res) -> {
            res.status(200);
            return "OK";
        });
    }

}
