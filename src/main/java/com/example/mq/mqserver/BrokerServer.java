package com.example.mq.mqserver;

import com.example.mq.common.*;
import com.example.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {
    private ServerSocket serverSocket = null;
    //一台服务器上只有一个virtualHost
    private VirtualHost virtualHost = new VirtualHost("default");
    //记录当前所有会话,key为channelId,value为对应的socket对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();
    //引入一个线程池,处理多个客户端的请求
    private ExecutorService executorService = null;
    //通过runnable控制服务器运行
    private volatile boolean runnable = true;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }
    public void start() throws IOException {
        System.out.println("[BorkerServer]启动!");
        executorService = Executors.newCachedThreadPool();
        try {
            while (runnable){
                Socket clientSocket = serverSocket.accept();
                executorService.submit(()->{
                    try {
                        processConnection(clientSocket);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("[BrokerServer]启动失败!类型转换错误!");
                    }
                });
            }
        }catch (SocketException e){
            System.out.println("[BrokerServer]服务器停止运行");
            //e.printStackTrace();
        }

    }

    //停止服务器
    public void stop() throws IOException {
        runnable = false;
        executorService.shutdownNow();
        serverSocket.close();
    }
    //通过这个方法处理一个客户端的连接
    private void processConnection(Socket clientSocket) throws ClassNotFoundException {
        try(InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream()){
            try(DataInputStream dataInputStream = new DataInputStream(inputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                while (true){
                    //1.读取请求并解析
                    Request request = readRequest(dataInputStream);
                    //2.根据请求计算响应
                    Response response = process(request,clientSocket);
                    //3.把响应返回给客户端
                    writeResponse(dataOutputStream,response);
                }
            }
        }catch (EOFException | SocketException e){
            System.out.println("[BrokerServer] connection 关闭!客户端地址:"+clientSocket.getInetAddress().toString()+":"+clientSocket.getPort());
        } catch (IOException | ClassNotFoundException | MqException e ) {
            System.out.println("[BrokerServer]connection出现异常!");
            e.printStackTrace();
        }finally {
            try {
                clientSocket.close();
                clearClosedSession(clientSocket);
            }catch (IOException e){
                e.printStackTrace();
            }
        }

    }

    private void clearClosedSession(Socket clientSocket) {
        //遍历sessions表,关闭socket对应的channel
        List<String> toDeleteChannelId = new ArrayList<>();
        for(Map.Entry<String,Socket> entry:sessions.entrySet()){
            if(entry.getValue() == clientSocket){
                toDeleteChannelId.add(entry.getKey());
            }
        }
        for(String channelId:toDeleteChannelId){
            sessions.remove(channelId);
        }
        System.out.println("[BrokerServer]清理session完成,被清理的channelId= "+toDeleteChannelId);
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        //刷新缓冲区,让数据从内存进入网卡
        dataOutputStream.flush();
    }

    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException, MqException {
        //1.把request中的payload做一个初步的解析
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid= "+basicArguments.getRid()+",channelId= "+basicArguments.getChannelId()+",type= " +
                ""+request.getType()+",length= "+request.getLength());
        //2.根据type的值进一步区分请求要干什么
        boolean ok = true;
        if(request.getType() == 0x1){
            //创建channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            System.out.println("[BrokerServer]创建Channel完成!channelId= "+basicArguments.getChannelId());
        }else if(request.getType() == 0x2){
            //销毁channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer]销毁Channel完成!channelId= "+basicArguments.getChannelId());
        }else if(request.getType() == 0x3){
            //创建交换机
            ExchangeDeclareArguments exchangeDeclareArguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(exchangeDeclareArguments.getExchangeName(),exchangeDeclareArguments.getExchangeType(),
                    exchangeDeclareArguments.isDurable(),exchangeDeclareArguments.isAutoDelete(),exchangeDeclareArguments.getArguments());
            System.out.println("[BrokerServer]创建交换机完成!exchangeName= "+exchangeDeclareArguments.getExchangeName());
        }else if(request.getType() == 0x4){
            //删除交换机
            ExchangeDeleteArguments exchangeDeleteArguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(exchangeDeleteArguments.getExchangeName());
            System.out.println("[BrokerServer]删除交换机完成!exchangeName= "+exchangeDeleteArguments.getExchangeName());
        }else if(request.getType() == 0x5){
            //创建消息队列
            MsgQueueDeclareArguments msgQueueDeclareArguments = (MsgQueueDeclareArguments) basicArguments;
            ok = virtualHost.msgQueueDeclare(msgQueueDeclareArguments.getQueueName(), msgQueueDeclareArguments.isAutoDelete(), msgQueueDeclareArguments.isDurable(),
                    msgQueueDeclareArguments.isExclusive(), msgQueueDeclareArguments.getArguments());
            System.out.println("[BrokerServer]创建消息队列完成!queueName= "+msgQueueDeclareArguments.getQueueName());
        }else if(request.getType() == 0x6){
            //删除消息队列
            MsgQueueDeleteArguments msgQueueDeleteArguments = (MsgQueueDeleteArguments) basicArguments;
            ok = virtualHost.msgQueueDelete(msgQueueDeleteArguments.getQueueName());
            System.out.println("[BrokerServer]删除消息队列完成!queueName= "+msgQueueDeleteArguments.getQueueName());
        }else if(request.getType() == 0x7){
            //创建binding
            QueueBindArguments queueBindArguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(queueBindArguments.getExchangeName(), queueBindArguments.getQueueName(), queueBindArguments.getBindingKey());
            System.out.println("[BrokerServer]创建binding完成!queueName= "+queueBindArguments.getQueueName()+",exchangeName= "+queueBindArguments.getExchangeName());
        }else if(request.getType() == 0x8){
            //删除binding
            QueueUnBindArguments queueUnBindArguments = (QueueUnBindArguments) basicArguments;
            ok = virtualHost.queueUnbind(queueUnBindArguments.getExchangeName(), queueUnBindArguments.getQueueName());
            System.out.println("[BrokerServer]删除binding完成!queueName= "+queueUnBindArguments.getQueueName()+",exchangeName= "+queueUnBindArguments.getExchangeName());
        }else if(request.getType() == 0x9){
            //发送消息
            BasicPublishArguments basicPublishArguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(basicPublishArguments.getExchangeName(), basicPublishArguments.getRoutingKey(), basicPublishArguments.getBasicProperties(),
                    basicPublishArguments.getBody());
            System.out.println("[BrokerServer]发送消息完成!");
        }else if(request.getType() == 0xa){
            //订阅队列
            BasicConsumeArguments basicConsumeArguments = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(basicConsumeArguments.getConsumerTag(), basicConsumeArguments.getQueueName(), basicConsumeArguments.isAutoAck(),
            new Consumer() {
                //服务器直接将消息写回给客户端
                @Override
                public void handlerDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                    //consumerTag为对应的channelId,根据channelId从sessions中找回socket,然后写回给客户端
                    Socket cliSocket = sessions.get(consumerTag);
                    if(cliSocket == null || cliSocket.isClosed()){
                        throw new MqException("[BrokerServer]订阅消息的客户端已经关闭!");
                    }
                    //构造响应数据
                    SubscribeReturns subscribeReturns = new SubscribeReturns();
                    subscribeReturns.setChannelId(consumerTag);
                    subscribeReturns.setBasicProperties(basicProperties);
                    subscribeReturns.setConsumerTag(consumerTag);
                    subscribeReturns.setRid(" ");
                    subscribeReturns.setOk(true);
                    subscribeReturns.setBody(body);
                    byte[] payload = BinaryTool.toByte(subscribeReturns);
                    Response response = new Response();
                    response.setType(0xc);
                    response.setLength(payload.length);
                    response.setPayload(payload);
                    //把消息写回给客户端,要多次给客户端写回消息,此处不关闭socket
                    DataOutputStream dataOutputStream = new DataOutputStream(cliSocket.getOutputStream());
                    writeResponse(dataOutputStream,response);
                }
            });
        }else if(request.getType() == 0xb){
            //调用basicAck确认消息
            BasicAckArguments basicAckArguments = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(basicAckArguments.getQueueName(), basicAckArguments.getMessageId());
        }else {
            throw new MqException("[BrokerServer]未知的type,type= "+request.getType());
        }
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ok);
        Response response = new Response();
        response.setType(request.getType());
        byte[] payload = BinaryTool.toByte(basicReturns);
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[Response] rid= "+basicReturns.getRid()+",channelId= "+basicReturns.getChannelId()+",type= "
                +response.getType()+",length= "+request.getLength());
        return response;
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload = new byte[request.getLength()];
        int n  = dataInputStream.read(payload);
        if(n != request.getLength()){
            throw new IOException("[BrokerServer]读取请求格式出错");
        }
        request.setPayload(payload);
        return request;
    }
}
