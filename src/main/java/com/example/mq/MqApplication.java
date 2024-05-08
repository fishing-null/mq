package com.example.mq;

import com.example.mq.mqserver.BrokerServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class MqApplication {
    public static ConfigurableApplicationContext context;


    public static void main(String[] args) throws IOException {

        context = SpringApplication.run(MqApplication.class, args);
        BrokerServer brokerServer = new BrokerServer(9090);
        brokerServer.start();
    }

}
