package com.example.mq.mqcilent;

import java.io.IOException;

public class ConnectionFactory {
    //brokerServer的ip
    private String host;
    //brokerServer的端口
    private int port;
    //访问brokerServer的哪个虚拟主机
//    private String virtualHostName;
//    private String userName;
//    private String password;


    public Connection newConnection() throws IOException {
        Connection connection = new Connection(host,port);
        return connection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
