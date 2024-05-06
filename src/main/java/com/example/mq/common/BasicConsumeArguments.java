package com.example.mq.common;

import java.io.Serializable;

public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
