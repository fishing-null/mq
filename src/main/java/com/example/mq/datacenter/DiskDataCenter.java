package com.example.mq.datacenter;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.List;

public class DiskDataCenter {
    private DataBaseManager dataBaseManager = new DataBaseManager();
    private MessageFileManager messageFileManager = new MessageFileManager();
    public void init(){
        dataBaseManager.init();
        messageFileManager.init();
    }
    //封装exchange相关
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
    }
    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
    }
    public List<Exchange> selectAllExchanges(){
        return dataBaseManager.selectAllExchanges();
    }
    //封装queue相关
    public void insertMsgQueue(MSGQueue msgQueue){
        dataBaseManager.insertQueue(msgQueue);
    }
    public void deleteMsgQueue(String queueName){
        dataBaseManager.deleteQueue(queueName);
    }
    public List<MSGQueue> selectAllMsgQueues(){
        return dataBaseManager.selectAllQueues();
    }
    //封装binding相关
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
    }
    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
    }
    public List<Binding> selectAllBindings(Binding binding){
        return dataBaseManager.selectAllEBindings();
    }
    //封装message相关
    public void sendMessage(MSGQueue msgQueue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(msgQueue,message);
    }
    public void deleteMessage(MSGQueue msgQueue,Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(msgQueue,message);
        if(messageFileManager.checkGC(msgQueue.getName())){
            messageFileManager.gc(msgQueue);
        }
    }
    public List<Message> loadAllMessagesFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }
}
