package org.zalando.nakadi4rich.model.view;

public class Partition {

    private String partition;
    private String oldestAvailableOffset;
    private String newestAvailableOffset;

    public Partition(final String partition, final String oldestAvailableOffset, final String newestAvailableOffset) {
        this.partition = partition;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
    }

    public String getPartition() {
        return partition;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }
}
