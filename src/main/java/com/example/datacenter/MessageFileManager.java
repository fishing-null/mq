package com.example.datacenter;

import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.Scanner;

public class MessageFileManager {
    public static class Stat{
        public int totalMessageCount;
        public int validMessageCount;
    }
    private String getQueuePath(String queueName){
        return "./data/" + queueName;
    }

    //获取消息文件路径 消息文件包含两部分 四字节定长表示消息长度 不定长长度表示消息体
    private String getQueueDataPath(String queueName){
        return getQueuePath(queueName) + "message_data.txt";
    }

    //获取消息统计文件路径 统计消息包含两个词条 总消息和有效消息
    private String getQueueStatsPath(String queueName){
        return getQueuePath(queueName) + "message_stats.txt";
    }
    //读取stats文件中数据
    private Stat readStat(String queueName){
        Stat stat = new Stat();
        try(InputStream inputStream = new FileInputStream(getQueueStatsPath(queueName))) {
            Scanner scanner = new Scanner(inputStream);
            stat.totalMessageCount = scanner.nextInt();
            stat.validMessageCount = scanner.nextInt();
            return stat;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    //写入stat文件中的数据
    private void writeStat(String queueName,Stat stat){
        try (OutputStream outputStream = new FileOutputStream(getQueueStatsPath(queueName))){
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalMessageCount + "\t" + stat.validMessageCount);
            printWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //创建队列对应的文件和目录
    public void createQueueDir(String queueName) throws IOException {
        File baseDir = new File(getQueuePath(queueName));
        //1.创建队列对应的目录文件
        if(!baseDir.exists()){
            boolean ok = baseDir.mkdirs();
            if(!ok){
                throw new IOException("创建目录失败!baseDir="+baseDir.getAbsolutePath());
            }
        }
        //2.创建队列数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        if(!queueDataFile.exists()){
            boolean ok = queueDataFile.mkdirs();
            if(!ok){
                throw new IOException("创建目录失败!queueDataFile="+queueDataFile.getAbsolutePath());
            }
        }
        //3.创建队列统计文件
        File queueStatFile = new File(getQueueStatsPath(queueName));
        if(!queueStatFile.exists()){
            boolean ok = queueStatFile.mkdirs();
            if(!ok){
                throw new IOException("创建目录失败!queueStatFile="+queueStatFile.getAbsolutePath());
            }
        }
        //给消息统计文件设计初始值 0 \t 0
        Stat stat = new Stat();
        stat.validMessageCount = 0;
        stat.totalMessageCount = 0;
        writeStat(queueName,stat);
    }


    //当队列被删,与之对应的消息也就没有作用了,删除消息目录
    private void destroyQueueDir(String queueName) throws IOException {
        File queueDirFile = new File(getQueuePath(queueName));
        boolean ok1 = queueDirFile.delete();
        File messageStatFile = new File(getQueueStatsPath(queueName));
        boolean ok2 = messageStatFile.delete();
        File messageDataFile = new File(getQueueDataPath(queueName));
        boolean ok3 = messageDataFile.delete();
        if(!ok1 || !ok2 || !ok3){
            throw new IOException("删除目录和文件失败!queueDir=" + messageDataFile.getAbsolutePath());
        }
    }
    //判断messageStatFile&messageDataFile目录文件是否存在
    private boolean checkFileExists(String queueName){
        File messageStatFile = new File(getQueueStatsPath(queueName));
        if(!messageStatFile.exists()){
            return false;
        }
        File messageDataFile = new File(getQueueDataPath(queueName));
        if(!messageDataFile.exists()){
            return false;
        }
        return true;
    }
    //将消息投入到队列中
    private void sendMessage(MSGQueue msgQueue, Message message) throws MqException, IOException {
        //1.检查消息文件是否存在
        if(!checkFileExists(msgQueue.getName())){
            throw new MqException("[MessageFileManager]-队列消息与文件不存在 queueName"+msgQueue.getName());
        }
        //2.把message对象转换成二进制数据
        byte[] messageBinary = BinaryTool.toByte(message);
        synchronized (msgQueue){
            //3.计算offsetBeg&offsetEnd
            //offsetBeg = 当前文件长度 + 4 offsetEnd = 当前文件长度 + 消息长度 + 4
            File queueDataFile = new File(getQueueDataPath(msgQueue.getName()));
            message.setOffsetBeg(queueDataFile.length() + 4);
            message.setOffsetEnd(queueDataFile.length() + messageBinary.length + 4);
            //4.将消息长度及消息写入文件
            try (OutputStream outputStream = new FileOutputStream(queueDataFile,true)){
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)){
                    //写入消息长度
                    dataOutputStream.writeInt(messageBinary.length);
                    //写入消息本体
                    dataOutputStream.write(messageBinary);
                }
            }
        }
        //5.更新消息统计文件
        Stat stat = readStat(msgQueue.getName());
        stat.totalMessageCount += 1;
        stat.validMessageCount += 1;
        writeStat(msgQueue.getName(),stat);
    }
}
