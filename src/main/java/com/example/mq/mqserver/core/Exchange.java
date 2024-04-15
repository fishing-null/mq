package com.example.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/*
 * 交换机
 */
public class Exchange {
    /*
     *交换机唯一标识符
     */
    private String name;
    /*
     * 交换机类型 DIRECT-0,FANOUT-1,TOPIC-2
     */
    private ExchangeType type;
    /*
     *交换机持久化保存选项 true-持久化保存 false-不持久化保存
     */
    private boolean durable;
    /*
     *是否在交换机不使用时自动删除
     */
    private boolean autoDelete;
    /*
     *创建交换机额外参数选项
     */
    private Map<String,Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getType() {
        return type;
    }

    public void setType(ExchangeType type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public void setArguments(String key,Object value){
        arguments.put(key, value);
    }
    public Object getArguments(String key){
        return arguments.get(key);
    }
    /*
     *数据库写入的时候会自动调用getter方法,需要将数据格式转换成数据库可以识别的格式,这里转换成json字符串的格式处理
     */
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    /*
     *从数据库读取数据之后,数据库会构造exchange对象,自动调用setter方法,需要在此处将json字符串转换成类中对应数据类型
     */
    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String,Object>>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
