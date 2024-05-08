package com.example.mq.mqcilent;

import com.example.mq.common.*;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {
    private String channelId;
    //当前channel属于哪一个连接
    private Connection connection;
    //存储客户端后续收到的响应 key-rid value-basicReturns
    private ConcurrentHashMap<String, BasicReturns> basicReturnsMap = new ConcurrentHashMap<>();
    //回调,一个channel只有一个回调
    private Consumer consumer = null;

    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }
    //通过这个方法在一个connection中创建一个或多个channel
    public boolean createChannel() throws IOException {
        //在这个方法中和服务器进行交互
        Request request = new Request();
        request.setType(0x1);
        //对于创建channel来说,payload此时是basicArguments;
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setChannelId(channelId);
        String rid = generatedRid();
        basicArguments.setRid(rid);
        byte[] payload = BinaryTool.toByte(basicArguments);
        request.setPayload(payload);
        request.setLength(payload.length);
        //发送请求
        connection.writeRequest(request);
        //阻塞等待并接受响应
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        //接收成功之后从map中移除
        return basicReturns.isOk();
    }

    //关闭channel
    public boolean closeChannel() throws IOException {
        BasicArguments basicArguments = new BasicArguments();
        String rid = generatedRid();
        basicArguments.setRid(rid);
        basicArguments.setChannelId(channelId);
        byte[] payload = BinaryTool.toByte(basicArguments);
        Request request = new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable,
                                   boolean autoDelete, Map<String,Object> arguments) throws IOException {
        ExchangeDeclareArguments exchangeDeclareArguments = new ExchangeDeclareArguments();
        String rid = generatedRid();
        exchangeDeclareArguments.setRid(rid);
        exchangeDeclareArguments.setChannelId(channelId);
        exchangeDeclareArguments.setExchangeName(exchangeName);
        exchangeDeclareArguments.setExchangeType(exchangeType);
        exchangeDeclareArguments.setDurable(durable);
        exchangeDeclareArguments.setAutoDelete(autoDelete);
        exchangeDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(exchangeDeclareArguments);
        Request request = new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(exchangeDeclareArguments.getRid());
        return basicReturns.isOk();
    }

    //删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments exchangeDeleteArguments = new ExchangeDeleteArguments();
        String rid = generatedRid();
        exchangeDeleteArguments.setChannelId(channelId);
        exchangeDeleteArguments.setRid(rid);
        exchangeDeleteArguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toByte(exchangeDeleteArguments);
        Request request = new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(exchangeDeleteArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean queueDeclare(String queueName,boolean durable,boolean autoDelete,boolean exclusive,Map<String,Object> arguments) throws IOException {
        MsgQueueDeclareArguments msgQueueDeclareArguments = new MsgQueueDeclareArguments();
        msgQueueDeclareArguments.setChannelId(channelId);
        String rid = generatedRid();
        msgQueueDeclareArguments.setRid(rid);
        msgQueueDeclareArguments.setQueueName(queueName);
        msgQueueDeclareArguments.setDurable(durable);
        msgQueueDeclareArguments.setAutoDelete(autoDelete);
        msgQueueDeclareArguments.setExclusive(exclusive);
        msgQueueDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(msgQueueDeclareArguments);
        Request request = new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(msgQueueDeclareArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean queueDelete(String queueName) throws IOException {
        MsgQueueDeleteArguments msgQueueDeleteArguments = new MsgQueueDeleteArguments();
        msgQueueDeleteArguments.setChannelId(channelId);
        String rid = generatedRid();
        msgQueueDeleteArguments.setRid(rid);
        msgQueueDeleteArguments.setQueueName(queueName);
        Request request = new Request();
        request.setType(0x6);
        byte[] payload = BinaryTool.toByte(msgQueueDeleteArguments);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(msgQueueDeleteArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean queueBind(String exchangeName,String queueName,String bindingKey ) throws IOException {
        QueueBindArguments queueBindArguments = new QueueBindArguments();
        String rid = generatedRid();
        queueBindArguments.setChannelId(channelId);
        queueBindArguments.setRid(rid);
        queueBindArguments.setExchangeName(exchangeName);
        queueBindArguments.setQueueName(queueName);
        queueBindArguments.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toByte(queueBindArguments);
        Request request = new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(queueBindArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean queueUnbind(String exchangeName,String queueName) throws IOException {
        QueueUnBindArguments queueUnBindArguments = new QueueUnBindArguments();
        String rid = generatedRid();
        queueUnBindArguments.setChannelId(channelId);
        queueUnBindArguments.setRid(rid);
        queueUnBindArguments.setExchangeName(exchangeName);
        queueUnBindArguments.setQueueName(queueName);
        byte[] payload = BinaryTool.toByte(queueUnBindArguments);
        Request request = new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(queueUnBindArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments basicPublishArguments = new BasicPublishArguments();
        String rid = generatedRid();
        basicPublishArguments.setChannelId(channelId);
        basicPublishArguments.setExchangeName(exchangeName);
        basicPublishArguments.setRid(rid);
        basicPublishArguments.setRoutingKey(routingKey);
        basicPublishArguments.setBasicProperties(basicProperties);
        basicPublishArguments.setBody(body);
        byte[] payload = BinaryTool.toByte(basicPublishArguments);
        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicPublishArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean basicConsume(String queueName,boolean autoAck,Consumer consumer) throws MqException, IOException {
        //先设置回调
        if(this.consumer != null){
            throw new MqException("该channel设置过消费消息的回调!不能重复设置");
        }
        this.consumer = consumer;
        BasicConsumeArguments basicConsumeArguments = new BasicConsumeArguments();
        String rid = generatedRid();
        basicConsumeArguments.setChannelId(channelId);
        basicConsumeArguments.setRid(rid);
        basicConsumeArguments.setConsumerTag(channelId);
        basicConsumeArguments.setQueueName(queueName);
        basicConsumeArguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toByte(basicConsumeArguments);
        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicConsumeArguments.getRid());
        return basicReturns.isOk();
    }

    public boolean basicAck(String queueName,String messageId) throws IOException {
        BasicAckArguments basicAckArguments = new BasicAckArguments();
        String rid = generatedRid();
        basicAckArguments.setChannelId(channelId);
        basicAckArguments.setRid(rid);
        basicAckArguments.setQueueName(queueName);
        basicAckArguments.setMessageId(messageId);
        byte[] payload = BinaryTool.toByte(basicAckArguments);
        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicAckArguments.getRid());
        return basicReturns.isOk();
    }
    //使用这个方法阻塞等待服务器的响应
    private BasicReturns waitResult(String rid) {
        BasicReturns basicReturns = null;
        while ((basicReturns = basicReturnsMap.get(rid)) == null){
            //如果查询为空,说明响应还没发送回来,需要阻塞等待
            synchronized (this){
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        basicReturnsMap.remove(rid);
        return basicReturns;
    }
    public void putReturns(BasicReturns basicReturns) {
        basicReturnsMap.put(basicReturns.getRid(),basicReturns);
        synchronized (this){
            notifyAll();
        }
    }
    private String generatedRid(){
        String rid = "R-"+ UUID.randomUUID().toString();
        return rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConcurrentHashMap<String, BasicReturns> getBasicReturnsMap() {
        return basicReturnsMap;
    }

    public void setBasicReturnsMap(ConcurrentHashMap<String, BasicReturns> basicReturnsMap) {
        this.basicReturnsMap = basicReturnsMap;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }


}
