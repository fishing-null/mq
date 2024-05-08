package com.example.mq.mqcilent;

import com.example.mq.common.Request;
import com.example.mq.common.Response;

import java.io.*;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Connection {
    private Socket socket = null;
    //key-channelId,value-channel对象
    private ConcurrentHashMap<String,Channel> channelMap = new ConcurrentHashMap<>();
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    public Connection(String host,int port) throws IOException {
        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
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
}
