package com.example.mq.mqserver.core;

import com.example.mq.common.ConsumerEnv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * 存储消息
 */
public class MSGQueue {
    /*
     * 队列唯一标识符
     */
    private String name;
    /*
     *队列在没有使用时是否自动删除,true-自动删除,false-不自动删除
     */
    private boolean autoDelete;
    /*
     *队列是否持久化存储,true-持久化存储,false-不持久化存储
     */
    private boolean durable;
    /*
     *是否独占队列,true-独占队列,false-不独占队列
     */
    private boolean exclusive;
    //当前队列有哪些消费者订阅了
    private List<ConsumerEnv> consumerEnvList  = new ArrayList<>();
    //记录当前取到第几个消费者
    private AtomicInteger consumerSeq = new AtomicInteger(0);
    private Map<String,Object> arguments;

    //添加一个新的订阅者
    public void addConsumerEnv(ConsumerEnv consumerEnv){
        consumerEnvList.add(consumerEnv);
    }

    //按照轮询的方式选择一个订阅者消费消息
    public ConsumerEnv ChoseConsumer(){
        if(consumerEnvList.size() == 0){
            //该队列没有消费者订阅
            return null;
        }
        int index = consumerSeq.get() % consumerEnvList.size();
        return consumerEnvList.get(index);
    }
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String,Object>>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }
}
