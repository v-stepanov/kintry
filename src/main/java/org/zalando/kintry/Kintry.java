package org.zalando.kintry;

import static spark.Spark.get;
import static spark.Spark.post;

public class Kintry {

    public static void main(String[] args) {
        get("/health", (req, res) -> {
            res.status(200);
            return "OK";
        });

        post("/events", (req, res) -> {
            final String body = req.body();
            System.out.println(body);
            res.status(201);
            return "";
        });
    }
}
