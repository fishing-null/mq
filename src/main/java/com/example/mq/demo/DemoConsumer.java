package com.example.mq.demo;

import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqcilent.Channel;
import com.example.mq.mqcilent.Connection;
import com.example.mq.mqcilent.ConnectionFactory;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/*
 * 这个类表示一个消费者
 * 通常是一个单独的服务器程序
 */
public class DemoConsumer {
    public static void main(String[] args) throws IOException, InterruptedException, MqException {
        System.out.println("启动消费者");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, true, null);
        channel.queueDeclare("testQueue", true, true, true, null);
        channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                System.out.println("[消费数据开始]");
                System.out.println("consumerTag= "+consumerTag);
                System.out.println("basicProperties= "+basicProperties);
                String bodyString = new String(body,0, body.length);
                System.out.println("body= "+bodyString);
                System.out.println("[消费数据结束]");
            }
        });
        //一直等待消费
        while (true){
            Thread.sleep(500);
        }
    }
}
