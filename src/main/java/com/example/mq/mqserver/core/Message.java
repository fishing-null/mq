package com.example.mq.mqserver.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

/*
 * 消息
 */
public class Message implements Serializable {
    private BasicProperties basicProperties = new BasicProperties();
    private byte[] body;
    //写入文件时,消息起始位置距离文件头部的位偏移
    private transient long offsetBeg;
    //写入文件时,消息终止位置距离文件头部的位偏移
    private transient long offsetEnd;
    //是否标记为逻辑删除 0x1-有效,0x0-无效
    private byte isValid = 0x1;

    /**
     * 工厂模式自动生成唯一id创建message
     * @param routingKey
     * @param basicProperties
     * @param body
     * @return
     */
    public static Message createMessageWithId(String routingKey,BasicProperties basicProperties,byte[] body){
        Message message = new Message();
        if(basicProperties != null){
            message.setBasicProperties(basicProperties);
        }
        message.setMessageId("M-" + UUID.randomUUID());
        message.setRoutingKey(routingKey);
        message.body = body;
        return message;
    }
    public String getMessageId(){
        return basicProperties.getMessageId();
    }
    public void  setMessageId(String messageId){
        basicProperties.setMessageId(messageId);
    }
    public String getRoutingKey(){
        return basicProperties.getRoutingKey();
    }
    public void setRoutingKey(String routingKey){
        basicProperties.setRoutingKey(routingKey);
    }

    public int getDeliverMode(){
        return basicProperties.getDeliverMode();
    }

    public void setDeliverMode(int deliverMode){
        basicProperties.setDeliverMode(deliverMode);
    }
    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }

    @Override
    public String toString() {
        return "Message{" +
                "basicProperties=" + basicProperties +
                ", body=" + Arrays.toString(body) +
                ", offsetBeg=" + offsetBeg +
                ", offsetEnd=" + offsetEnd +
                ", isValid=" + isValid +
                '}';
    }
}
