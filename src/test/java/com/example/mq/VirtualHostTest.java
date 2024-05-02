package com.example.mq;

import com.example.mq.common.Consumer;
import com.example.mq.mqserver.VirtualHost;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class VirtualHostTest {
    private VirtualHost testVirtualHost = null;
    @BeforeEach
    public void setUp(){
        MqApplication.context = SpringApplication.run(MqApplication.class);
        testVirtualHost = new VirtualHost("testVirtualHost");
    }

    @AfterEach
    public void teatDown() throws IOException {
        MqApplication.context.close();
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }

    @Test
    public void testExchangeDeclare(){
        boolean ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
    }
    @Test
    public void testExchangeDelete(){
        boolean ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueDeclare(){
         boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
         ,null);
         Assertions.assertTrue(ok);
    }
    @Test
    public void testQueueDelete(){
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.msgQueueDelete("testQueue");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueBinding(){
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.queueBind("testExchange","testQueue",
                "testBindingKey");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueUnbind(){
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.queueBind("testExchange","testQueue",
                "testBindingKey");
        Assertions.assertTrue(ok);
        ok = testVirtualHost.queueUnbind("testExchange","testQueue");
        Assertions.assertTrue(ok);
    }

    @Test
    public void  basicPublish(){
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    @Test
    public void testBasicConsume1() throws InterruptedException {
        //先发消息后订阅队列
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Thread.sleep(500);
        ok = testVirtualHost.basicConsume("testConsumerTag", "testQueue", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("messageId= "+basicProperties.getMessageId());
                System.out.println("body="+new String(body,0,body.length));
                Assertions.assertEquals("testQueue",basicProperties.getRoutingKey());
                Assertions.assertEquals("1",basicProperties.getDeliverMode());
                Assertions.assertArrayEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
    }

    @Test
    public void testBasicConsume2() throws InterruptedException {
        //先订阅队列后发送消息
        boolean ok = testVirtualHost.msgQueueDeclare("testQueue",true,true,true
                ,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.basicConsume("testConsumerTag", "testQueue", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("messageId= "+basicProperties.getMessageId());
                System.out.println("body="+new String(body,0,body.length));
                Assertions.assertEquals("testQueue",basicProperties.getRoutingKey());
                Assertions.assertEquals("1",basicProperties.getDeliverMode());
                Assertions.assertArrayEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        //发送消息
        ok = testVirtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Assertions.assertTrue(ok);
    }
    @Test
    public void basicConsumenFanout() throws InterruptedException {
        boolean ok = testVirtualHost.exchangeDeclare("testExchange", ExchangeType.FANOUT,false,
                false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.msgQueueDeclare("testQueue1",false,false,false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.queueBind("testExchange","testQueue1","");
        Assertions.assertTrue(ok);
        ok = testVirtualHost.msgQueueDeclare("testQueue2",false,false,false,null);
        Assertions.assertTrue(ok);
        ok = testVirtualHost.queueBind("testExchange","testQueue2","");
        Assertions.assertTrue(ok);
        //往交换机中发布消息
        ok = testVirtualHost.basicPublish("testExchange","",null,"hello".getBytes());
        //消费消息
        ok = testVirtualHost.basicConsume("consumerTag1", "testQueue1", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("messageId= "+basicProperties.getMessageId());
                System.out.println("consumerTag= "+consumerTag);
                Assertions.assertArrayEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        ok = testVirtualHost.basicConsume("consumerTag2", "testQueue2", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("messageId= "+basicProperties.getMessageId());
                System.out.println("consumerTag= "+consumerTag);
                Assertions.assertArrayEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);
    }
}
