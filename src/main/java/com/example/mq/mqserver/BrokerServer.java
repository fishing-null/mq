package com.example.mq.mqserver;

import com.example.mq.common.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
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
        while (runnable){
            Socket clientSocket = serverSocket.accept();
            executorService.submit(()->{
                processConnection(clientSocket);
            });
        }
    }

    //停止服务器
    public void stop() throws IOException {
        runnable = false;
        executorService.shutdownNow();
        serverSocket.close();
    }
    //通过这个方法处理一个客户端的连接
    private void processConnection(Socket clientSocket) {
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
        } catch (IOException e) {
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
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        //刷新缓冲区,让数据从内存进入网卡
        dataOutputStream.flush();
    }

    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException {
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
        }
        return null;
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload = new byte[request.getLength()];
        int n  = dataInputStream.read(payload);
        if(n != request.getLength()){
            throw new IOException("读取请求格式出错");
        }
        request.setPayload(payload);
        return request;
    }
}
