package com.example.mq.mqserver.core;

import com.example.mq.common.Consumer;
import com.example.mq.common.ConsumerEnv;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.VirtualHost;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class ConsumerManager {
    //持有上层的VirtualHost,用来操作数据
    private VirtualHost parent;
    //指定一个线程池,负责执行具体的回调任务
    private ExecutorService workPool = Executors.newFixedThreadPool(4);
    //存放令牌的队列
    private BlockingDeque<String> tokenQueue = new LinkedBlockingDeque<>();

    private Thread searchThread = null;
    public ConsumerManager(VirtualHost p) {
        parent = p;
    }
    //在发送消息时调用
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    //新增consumer对象到指定的队列对象当中
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MqException {
        //获取到指定的队列
        MSGQueue msgQueue = parent.getMemoryDataCenter().getMsgQueue(queueName);
        if(msgQueue == null){
            throw new MqException("[ConsumerManager]队列不存在,queueName= "+queueName);
        }
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        synchronized (msgQueue){
            msgQueue.addConsumerEnv(consumerEnv);
            //队列中以及有消息了,需要立即消费掉
            int n = parent.getMemoryDataCenter().getMessageCount(queueName);
            for (int i = 0; i < n; i++) {
                //调用一次就消费一条消息
                consumeMessage(msgQueue);

            }
        }
    }

    private void consumeMessage(MSGQueue msgQueue) {

    }
}
