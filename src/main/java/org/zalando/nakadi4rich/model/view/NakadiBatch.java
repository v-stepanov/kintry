package org.zalando.nakadi4rich.model.view;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public class NakadiBatch {

    private Cursor cursor;

    private List<String> events;

    @JsonCreator
    public NakadiBatch(@JsonProperty("cursor") final Cursor cursor, @JsonProperty("events") final List<String> events) {
        this.cursor = cursor;
        this.events = ImmutableList.copyOf(events);
    }

    public NakadiBatch(final String partition, final String offset, final List<String> events) {
        this.cursor = new Cursor(partition, offset);
        this.events = ImmutableList.copyOf(events);
    }

    public Cursor getCursor() {
        return cursor;
    }

    public List<String> getEvents() {
        return Collections.unmodifiableList(events);
    }

    public static class Cursor {

        private String partition;

        private String offset;

        @JsonCreator
        public Cursor(@JsonProperty("partition") final String partition, @JsonProperty("offset") final String offset) {
            this.partition = partition;
            this.offset = offset;
        }

        public String getPartition() {
            return partition;
        }

        public String getOffset() {
            return offset;
        }
    }
}
