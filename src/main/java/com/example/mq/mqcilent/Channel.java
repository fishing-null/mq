package com.example.mq.mqcilent;

import com.example.mq.common.BasicReturns;
import com.example.mq.common.Consumer;

import java.util.concurrent.ConcurrentHashMap;

public class Channel {
    private String channelId;
    //当前channel属于哪一个连接
    private Connection connection;
    //存储客户端后续收到的响应
    private ConcurrentHashMap<String, BasicReturns> basicReturnsMap = new ConcurrentHashMap<>();
    //回调,一个channel只有一个回调
    private Consumer consumer = null;

    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }
}
