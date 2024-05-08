package com.example.mq;

import com.example.mq.mqcilent.Connection;
import com.example.mq.mqcilent.ConnectionFactory;
import com.example.mq.mqserver.BrokerServer;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
}
