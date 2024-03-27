package com.example.datacenter;

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

    //获取消息文件路径
    private String getMessageDataPath(String queueName){
        return getQueuePath(queueName) + "message_data.txt";
    }

    //获取消息统计文件路径
    private String getMessageStatsPath(String queueName){
        return getQueuePath(queueName) + "message_stats.txt";
    }
    //读取stats文件中数据
    private Stat readStat(String queueName){
        Stat stat = new Stat();
        try(InputStream inputStream = new FileInputStream(getMessageStatsPath(queueName))) {
            Scanner scanner = new Scanner(inputStream);
            stat.totalMessageCount = scanner.nextInt();
            stat.validMessageCount = scanner.nextInt();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    //写入stat文件中的数据
    private void writeStat(String queueName,Stat stat){
        try (OutputStream outputStream = new FileOutputStream(getMessageStatsPath(queueName))){
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalMessageCount + "\t" + stat.validMessageCount);
            printWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //当队列被删,与之对应的消息也就没有作用了,删除消息目录
    private void destroyQueueDir(String queueName) throws IOException {
        File queueDirFile = new File(getQueuePath(queueName));
        boolean ok1 = queueDirFile.delete();
        File messageStatFile = new File(getMessageStatsPath(queueName));
        boolean ok2 = messageStatFile.delete();
        File messageDataFile = new File(getMessageDataPath(queueName));
        boolean ok3 = messageDataFile.delete();
        if(!ok1 || !ok2 || !ok3){
            throw new IOException("删除目录和文件失败!queueDir=" + messageDataFile.getAbsolutePath());
        }
    }
    //判断messageStatFile&messageDataFile目录文件是否存在
    private boolean checkFileExists(String queueName){
        File messageStatFile = new File(getMessageStatsPath(queueName));
        if(!messageStatFile.exists()){
            return false;
        }
        File messageDataFile = new File(getMessageDataPath(queueName));
        if(!messageDataFile.exists()){
            return false;
        }
        return true;
    };
}
