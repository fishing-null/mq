package com.example.mq.mqcilent;

import com.example.mq.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
    private Socket socket = null;
    //key-channelId,value-channel对象
    private ConcurrentHashMap<String,Channel> channelMap = new ConcurrentHashMap<>();
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    private ExecutorService callbackPool = null;
    public Connection(String host,int port) throws IOException {
        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
        callbackPool = Executors.newFixedThreadPool(4);
        //创建一个扫描线程,扫描线程从socket中读取数据,再把数据交给对应的channel处理
        Thread t = new Thread(()->{
            while (!socket.isClosed()){
                try {
                    Response response = readResponse();
                    dispatchResponse(response);
                } catch (SocketException e) {
                    System.out.println("[Connection]连接正常断开");
                } catch (ClassNotFoundException | IOException |  MqException e){
                    System.out.println("[Connection]连接异常断开");
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MqException {
        //分别处理控制请求响应和消息响应
        if(response.getType() == 0xc){
            //服务器推送来的消息数据
            SubscribeReturns subscribeReturns = (SubscribeReturns) BinaryTool.fromBytes(response.getPayload());
            //根据channelId找到对应的channel对象
            Channel channel = channelMap.get(subscribeReturns.getChannelId());
            if(channel == null){
                throw new MqException("[Connection] 与该消息对应的channel在客户端中不存在!channelId= "+channel.getChannelId());
            }
            //执行channel内部的回调
            callbackPool.submit(()->{
                try {
                    channel.getConsumer().handlerDelivery(subscribeReturns.getConsumerTag(),subscribeReturns.getBasicProperties(), subscribeReturns.getBody());
                } catch (MqException | IOException e) {
                    e.printStackTrace();
                }
            });
        }else {
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());
            Channel channel = channelMap.get(basicReturns.getChannelId());
            if(channel == null){
                throw new MqException("[Connection] 与该消息对应的channel在客户端中不存在!channelId= "+channel.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    //发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        System.out.println("[Connection]发送请求,type= "+request.getType()+",length= "+request.getLength());
    }
    //读取响应
    public Response readResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int n  = dataInputStream.read(payload);
        if(n != response.getLength()){
            throw new IOException("[Connection]读取的响应数据不完整!");
        }
        response.setPayload(payload);
        System.out.println("[Connection]接收响应,type= "+response.getType()+",length= "+response.getLength());
        return response;
    }

    //通过这个方法在Connection中创建channel
    public Channel createChannel() throws IOException {
        String channelId = "C-"+UUID.randomUUID().toString();
        Channel channel = new Channel(channelId,this);
        //把这个connection管理的所有channel对象放到hashmap中
        channelMap.put(channelId,channel);
        //把创建channel同步给服务器
        boolean ok = channel.createChannel();
        if(!ok){
            //创建channel失败,把刚才加入hashmap的键值对删除
            channelMap.remove(channelId);
            return null;
        }
        return channel;
    }

    public void close(){
        //关闭Connection释放上述资源
        try {
            callbackPool.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
