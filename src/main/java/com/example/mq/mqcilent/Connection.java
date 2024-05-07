package com.example.mq.mqcilent;

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

}
