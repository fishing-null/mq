package com.example.mq.mqcilent;

import com.example.mq.common.Request;
import com.example.mq.common.Response;

import java.io.*;
import java.net.Socket;
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
}
