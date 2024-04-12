package com.example.mq.mqserver.datacenter;

import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDataCenter {
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();
    //表示队列和消息的关联
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();
    //表示未确认消息
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();
}
