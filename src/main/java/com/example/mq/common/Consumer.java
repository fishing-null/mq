package com.example.mq.common;

import com.example.mq.mqserver.core.BasicProperties;

import java.io.IOException;

@FunctionalInterface
public interface Consumer {
    //delivery-投递 在每次服务器收到消息之后调用,通过这个方法把消息推送给对应的消费者
    void handlerDelivery(String consumerTag, BasicProperties basicProperties,byte[] body) throws MqException, IOException;
}
