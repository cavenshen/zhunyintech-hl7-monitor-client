package com.zhunyintech.inmehl7.client.model;

public class OutboxRecord {
    private long outboxId;
    private long headerId;
    private int retryCount;
    private Hl7ResultRecord record;

    public long getOutboxId() {
        return outboxId;
    }

    public void setOutboxId(long outboxId) {
        this.outboxId = outboxId;
    }

    public long getHeaderId() {
        return headerId;
    }

    public void setHeaderId(long headerId) {
        this.headerId = headerId;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Hl7ResultRecord getRecord() {
        return record;
    }

    public void setRecord(Hl7ResultRecord record) {
        this.record = record;
    }
}

