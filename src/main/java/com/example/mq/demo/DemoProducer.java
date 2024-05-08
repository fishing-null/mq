package com.example.mq.demo;

import com.example.mq.mqcilent.Channel;
import com.example.mq.mqcilent.Connection;
import com.example.mq.mqcilent.ConnectionFactory;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/*
 * 这个类表示一个生产者
 * 通常是一个单独的服务器程序
 */
public class DemoProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,true,null);
        channel.queueDeclare("testQueue",true,true,true,null);
        byte[] body = "hello".getBytes();
        boolean ok = channel.basicPublish("testExchange","testQueue",null,body);
        System.out.println("消息投递完成!ok= "+ok);
        Thread.sleep(500);
        channel.closeChannel();
        connection.close();
    }

}
