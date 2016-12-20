package org.zalando.kintry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class NakadiBatch {

    private Cursor cursor;

    private List<String> events;

    @JsonCreator
    public NakadiBatch(final Cursor cursor, final List<String> events) {
        this.cursor = cursor;
        this.events = ImmutableList.copyOf(events);
    }

    public Cursor getCursor() {
        return cursor;
    }

    public List<String> getEvents() {
        return events;
    }

    public static class Cursor {

        private String partition;

        private String offset;

        @JsonCreator
        public Cursor(final String partition, final String offset) {
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
