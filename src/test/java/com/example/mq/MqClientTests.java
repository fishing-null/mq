package com.example.mq;

import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqcilent.Channel;
import com.example.mq.mqcilent.Connection;
import com.example.mq.mqcilent.ConnectionFactory;
import com.example.mq.mqserver.BrokerServer;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;

public class MqClientTests {
    private BrokerServer brokerServer = null;
    private ConnectionFactory connectionFactory = null;
    private Thread t = null;
    @BeforeEach
    public void setUp() throws IOException {
        //1.先启动服务器
        MqApplication.context = SpringApplication.run(MqApplication.class);
        brokerServer = new BrokerServer(9090);
        t = new Thread(()->{
            try {
                brokerServer.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();
        //2.配置connectionFactory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

    }
    @AfterEach
    public void tearDown() throws IOException {
        //停止服务器
        brokerServer.stop();
        MqApplication.context.close();
        //删除必要的文件
        File file = new File("./data");
        FileUtils.deleteDirectory(file);

        connectionFactory = null;
    }
    @Test
    public void testConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
    }

    @Test
    public void testChannel() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }
    @Test
    public void testExchange() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,true,null);
        Assertions.assertTrue(ok);
        ok = channel.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testQueue() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.queueDeclare("testQueue",true,true,true,null);
        Assertions.assertTrue(ok);
        ok = channel.queueDelete("testQueue");
        Assertions.assertTrue(ok);
        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testBinding() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();

        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,true,null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue",true,true,true,null);
        Assertions.assertTrue(ok);
        ok = channel.queueBind("testExchange","testQueue","testBindingKey");
        Assertions.assertTrue(ok);
        ok = channel.queueUnbind("testExchange","testQueue");
        Assertions.assertTrue(ok);
        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testMessage() throws IOException, MqException, InterruptedException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();


        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,true,null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue",true,true,true,null);

        byte[] requestBody = "hello".getBytes();
        ok = channel.basicPublish("testExchange","testQueue",null,requestBody);
        Assertions.assertTrue(ok);
        ok = channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                System.out.println("消费数据开始!");
                System.out.println("consumerTag= "+consumerTag);
                System.out.println("basicProperties= "+basicProperties);
                Assertions.assertArrayEquals(requestBody,body);
                System.out.println("消费数据结束!");
            }
        });

        Assertions.assertTrue(ok);
        Thread.sleep(500);

        channel.closeChannel();
        connection.close();
    }
}
