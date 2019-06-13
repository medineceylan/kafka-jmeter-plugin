package com.kafka.jmeter.plugin.model;

public class Message {
    private String batch;
    private Integer threadNumber;
    private Integer messageNumber;
    private Long created;

    public Message(final String batch, final Integer threadNumber, final Integer messageNumber, final Long created) {
        this.batch = batch;
        this.threadNumber = threadNumber;
        this.messageNumber = messageNumber;
        this.created = created;
    }

    public Message(){

    }
    public Integer getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(Integer threadNumber) {
        this.threadNumber = threadNumber;
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
    }

    public Integer getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(Integer messageNumber) {
        this.messageNumber = messageNumber;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    @Override
    public String toString() {
        return "Message{" +
                "batch:" + batch +
                ", threadNumber:" + threadNumber +
                ", messageNumber:" + messageNumber +
                ", created:'" + created + '\'' +
                '}';
    }
}
