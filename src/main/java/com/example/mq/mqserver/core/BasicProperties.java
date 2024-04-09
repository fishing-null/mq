package com.example.mq.mqserver.core;

import java.io.Serializable;

public class BasicProperties implements Serializable {
    //message唯一标识符
    private String messageId;
    //和bindingKey做匹配
    //direct-routingKey为转发的队列名称
    //fanout-routingKey无意义
    //topic-routingKey与bindingKey做匹配,转发给所有符合要求的队列
    private String routingKey;
    //传递模式 消息是否持久化 1-不持久化 2-持久化
    private int deliverMode;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDeliverMode() {
        return deliverMode;
    }

    public void setDeliverMode(int deliverMode) {
        this.deliverMode = deliverMode;
    }

    @Override
    public String toString() {
        return "BasicProperties{" +
                "messageId='" + messageId + '\'' +
                ", routingKey='" + routingKey + '\'' +
                ", deliverMode=" + deliverMode +
                '}';
    }
}
