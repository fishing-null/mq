package com.example.mq.mqserver.datacenter;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDataCenter {
    //在内存中管理相关数据
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();
    //表示队列和消息的关联 key是队列名字.value是Message组成的链表
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();
    //表示未确认消息 key是队列名字,value是<messageId,Message>
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();

    public void insertExchange(Exchange exchange){
        exchangeMap.put(exchange.getName(),exchange);
        System.out.println("[MemoryDataCenter]新交换机添加成功!exchangeName="+exchange.getName());
    }
    public void deleteExchange(String exchangeName) throws MqException {
        if(exchangeMap.get(exchangeName) != null){
            exchangeMap.remove(exchangeName);
            System.out.println("[MemoryDataCenter]交换机删除成功!exchangeName="+exchangeName);
            return;
        }
        throw new MqException("[MemoryDataCenter]交换机删除失败!exchangeName="+exchangeName);
    }

    public void getExchange(String exchangeName){
        exchangeMap.get(exchangeName);
        System.out.println("[MemoryDataCenter]交换机删除成功!exchangeName="+exchangeName);
    }

    public void insertMsgQueue(MSGQueue msgQueue){
        queueMap.put(msgQueue.getName(),msgQueue);
        System.out.println("[MemoryDataCenter]新队列添加成功!queueName="+msgQueue.getName());
    }
    public void getMsgQueue(String queueName){
        queueMap.get(queueName);
    }
    public void deleteMsgQueue(String queueName) throws MqException {
        if(queueMap.get(queueName) != null){
            queueMap.remove(queueName);
            System.out.println("[MemoryDataCenter]队列删除成功!queueName="+queueName);
            return;
        }
        throw new MqException("[MemoryDataCenter]队列删除失败!,queueName="+queueName);

    }

    public void insertBinding(Binding binding) throws MqException {
        //使用exchangeName查询对应的bindingMap是否存在,不存在就创建一个
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(),k -> new ConcurrentHashMap<>());

        synchronized (bindingMap){
            if(bindingMap.get(binding.getQueueName()) != null){
                throw new MqException("[MemoryDataCenter]绑定已经存在!exchangeName="+binding.getExchangeName()+", queueName="+binding.getQueueName());
            }
            bindingMap.put(binding.getQueueName(),binding);
            System.out.println("[MemoryDataCenter]绑定添加成功!exchangeName="+binding.getExchangeName()+", queueName="+binding.getQueueName());
        }
    }

    //根据ExchangeName&QueueName,确定唯一一个binding
    public Binding getBinding(String exchangeName,String queueName){
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.get(exchangeName);
        if(bindingMap == null){
            return null;
        }
        return bindingMap.get(queueName);
    }
    //根据ExchangeName,获取所有的binding
    public ConcurrentHashMap<String,Binding> getAllBindings(String exchangeName){
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.get(exchangeName);
        return bindingMap;
    }

    public void deleteBinding(Binding binding) throws MqException {
        //根据binding的exchangeName找到绑定,再对其进行删除
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if(bindingMap == null){
            throw new MqException("[MemoryDataCenter]绑定不存在,exchangeName="+binding.getExchangeName()+",queueName="+binding.getQueueName());
        }
        bindingMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataCenter]绑定删除成功!exchangeName="+binding.getExchangeName()+", queueName="+binding.getQueueName());
    }

    public void insertMessage(Message message){
        messageMap.put(message.getMessageId(),message);
        System.out.println("[MemoryDataCenter]新消息添加成功!messageId="+message.getMessageId());
    }

    public Message getMessage(String messageId){
        return messageMap.get(messageId);
    }

    public void deleteMessage(String messageId) throws MqException {
        if(messageMap.get(messageId) != null){
            messageMap.remove(messageId);
            System.out.println("[MemoryDataCenter]消息被移除!messageId="+messageId);
            return;
        }
        throw new MqException("[MemoryDataCenter]消息移除失败!messageId="+messageId);
    }

    //发送消息到指定队列
    public void sendMessage(MSGQueue msgQueue,Message message){
        //根据queueName找到该队列的消息链表
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(msgQueue.getName(), k -> new LinkedList<>());
        //将消息添加到队列中
        synchronized (messages){
            messages.add(message);
        }
        insertMessage(message);
        System.out.println("[MemoryDataCenter]消息被添加到队列中!messageId="+message.getMessageId());
    }

    //从指定队列取出消息
    public Message pollMessage(String queueName){
        //根据队列名,查找对应队列的消息链表
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null ){
            return null;
        }
        synchronized (messages){
            if(messages.size() == 0){
                return null;
            }
            Message currentMessage = messages.remove(0);
            System.out.println("[MemoryDataCenter]消息从队列中取出!messageId="+currentMessage.getMessageId());
            return currentMessage;
        }
    }
    //获取指定队列的消息个数
    public int getMessageCount(String queueName){
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null){
            return 0;
        }
        synchronized (messages){
            return messages.size();
        }
    }

    //添加未确认消息
    public void insertMessageWaitAck(String queueName,Message message){
        ConcurrentHashMap<String,Message> messageHashMap = queueMessageWaitAckMap.computeIfAbsent(queueName,k -> new ConcurrentHashMap<>());
        messageHashMap.put(message.getMessageId(),message);
        System.out.println("[MemoryDataCenter]消息向待确认队列添加成功!messageId="+message.getMessageId());
    }

    //删除未确认的消息(已经确认了)
    public void deleteMessageWaitAck(String queueName,String messageId) throws MqException {
        ConcurrentHashMap<String,Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if(messageHashMap == null){
            throw new MqException("[MemoryDataCenter]消息从待确认队列删除失败!messageId="+messageId);
        }
        messageHashMap.remove(messageId);
        System.out.println("[MemoryDataCenter]消息从待确认队列删除成功!messageId="+messageId);
    }

    //获取未确认的消息
    public Message getMessageWaitAck(String queueName,String messageId){
        ConcurrentHashMap<String,Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if(messageHashMap == null){
            return null;
        }
        return messageHashMap.get(messageId);
    }

    //从硬盘恢复内存到数据
    public void recovery(DiskDataCenter diskDataCenter) throws IOException, MqException, ClassNotFoundException {
        //清除之前旧的数据
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        messageMap.clear();
        queueMessageMap.clear();
        //1.恢复所有的交换机数据
        List<Exchange> exchangeList = diskDataCenter.selectAllExchanges();
        for (Exchange exchange:exchangeList) {
            exchangeMap.put(exchange.getName(), exchange);
        }
        //2.恢复所有的队列数据
        List<MSGQueue> queueList = diskDataCenter.selectAllMsgQueues();
        for(MSGQueue queue:queueList){
            queueMap.put(queue.getName(),queue);
        }

        //3.恢复所有的绑定数据
        List<Binding> bindingList = diskDataCenter.selectAllBindings();
        for(Binding binding:bindingList){
            ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(),k -> new ConcurrentHashMap<>());
            bindingMap.put(binding.getQueueName(), binding);
        }

        //4.恢复所有的消息数据
        for(MSGQueue queue:queueList){
            LinkedList<Message> messageList = diskDataCenter.loadAllMessagesFromQueue(queue.getName());
            queueMessageMap.put(queue.getName(), messageList);
            for(Message message:messageList){
                messageMap.put(message.getMessageId(),message);
            }
        }
    }

}
