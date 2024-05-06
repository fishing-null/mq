package com.example.mq.common;

import com.example.mq.mqserver.core.BasicProperties;

import java.io.Serializable;

public class SubscribeReturns extends BasicReturns implements Serializable {
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[] bytes;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
}
