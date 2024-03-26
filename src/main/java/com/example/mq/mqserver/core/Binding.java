package com.example.mq.mqserver.core;

public class Binding {
    /*
     *绑定的交换机名称
     */
    private String exchangeName;
    /*
     *绑定的队列名称
     */
    private String queueName;
    private String bindingKey;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
