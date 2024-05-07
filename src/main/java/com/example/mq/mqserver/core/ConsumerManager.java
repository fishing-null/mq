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

    private Thread scannerThread = null;
    public ConsumerManager(VirtualHost p) {
        parent = p;
        scannerThread = new Thread(()->{
            while (true){
                try {
                    //1.拿到令牌
                    String queueName = tokenQueue.take();
                    //2.根据令牌拿到队列
                    MSGQueue msgQueue = parent.getMemoryDataCenter().getMsgQueue(queueName);
                    if(msgQueue == null){
                        throw new MqException("[ConsumerManager] 取令牌后,发现该队列名不存在!queueName= "+queueName);
                    }
                    //3.从队列中消费消息
                    synchronized (msgQueue){
                        consumeMessage(msgQueue);
                    }
                } catch (InterruptedException | MqException e){
                    e.printStackTrace();
                }
            }
        });
        //把线程设为后台线程
        scannerThread.setDaemon(true);
        scannerThread.start();
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
        //1.按照轮询的方式,选择一个消费者
        ConsumerEnv luckyDog = msgQueue.chooseConsumer();
        if(luckyDog == null){
            //当前队列没有消费者,暂时不消费
            return;
        }
        //2.从队列中取出一个消息
        Message message = parent.getMemoryDataCenter().pollMessage(msgQueue.getName());
        if(message == null){
            //当前队列中没有消息,暂时不消费
            return;
        }
        //3.把消息带入到消费者的回调方法中,丢给线程池执行
        workPool.submit(()->{
            try{
                //1.把消息放到待确认的集合中
                parent.getMemoryDataCenter().insertMessageWaitAck(msgQueue.getName(), message);
                //2.执行回调操作
                luckyDog.getConsumer().handlerDelivery(luckyDog.getConsumerTag(), message.getBasicProperties(), message.getBody());
                //3.自动应答直接删除消息,手动应答调用basicAck方法
                if(luckyDog.isAutoAck()){
                    //1.删除硬盘上的消息
                    if(message.getDeliverMode() == 2){
                        parent.getDiskDataCenter().deleteMessage(msgQueue,message);
                    }
                    //2.删除待确认集合上的消息
                    parent.getMemoryDataCenter().deleteMessageWaitAck(msgQueue.getName(), message.getMessageId());
                    //3.删除内存中的消息
                    parent.getMemoryDataCenter().deleteMessage(message.getMessageId());
                    System.out.println("[ConsumerManager]消息被成功消费!queueName="+msgQueue.getName());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("[ConsumerManager]消息成功被消费!queueName = "+msgQueue.getName());
        });
    }
}
