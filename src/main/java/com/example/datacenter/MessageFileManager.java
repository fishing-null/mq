package com.example.datacenter;

import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class MessageFileManager {
    public void init() {
    }

    public static class Stat{
        public int totalMessageCount;
        public int validMessageCount;
    }
    private String getQueuePath(String queueName){
        return "./data/" + queueName;
    }

    //获取消息文件路径 消息文件包含两部分 四字节定长表示消息长度 不定长长度表示消息体
    private String getQueueDataPath(String queueName){
        return getQueuePath(queueName) + "/queue_data.txt";
    }

    //获取消息统计文件路径 统计消息包含两个词条 总消息和有效消息
    private String getQueueStatsPath(String queueName){
        return getQueuePath(queueName) + "/queue_stat.txt";
    }
    //更具队列名称,读取stats文件中数据
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
            boolean ok = queueDataFile.createNewFile();
            if(!ok){
                throw new IOException("创建文件失败!queueDataFile="+queueDataFile.getAbsolutePath());
            }
        }
        //3.创建队列统计文件
        File queueStatFile = new File(getQueueStatsPath(queueName));
        if(!queueStatFile.exists()){
            boolean ok = queueStatFile.createNewFile();
            if(!ok){
                throw new IOException("创建文件失败!queueStatFile="+queueStatFile.getAbsolutePath());
            }
        }
        //给消息统计文件设计初始值 0 \t 0
        Stat stat = new Stat();
        stat.validMessageCount = 0;
        stat.totalMessageCount = 0;
        writeStat(queueName,stat);
    }


    //当队列被删,与之对应的消息也就没有作用了,删除消息目录
    public void destroyQueueDir(String queueName) throws IOException {
        //注意删除顺序,先删除文件,在删除目录

        File messageStatFile = new File(getQueueStatsPath(queueName));
        boolean ok1 = messageStatFile.delete();
        File messageDataFile = new File(getQueueDataPath(queueName));
        boolean ok2 = messageDataFile.delete();
        File queueDirFile = new File(getQueuePath(queueName));
        boolean ok3 = queueDirFile.delete();
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
    public void sendMessage(MSGQueue msgQueue, Message message) throws MqException, IOException {
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
    //从文件中逻辑删除队列中的一条消息
    //1.把文件中的数据读出来,还原成Message对象
    //2.将message对象中的isValid字段更改为0
    //3.将message对象序列化,重新写入文件
    //4.更新统计文件
    public void deleteMessage(MSGQueue msgQueue,Message message) throws IOException, ClassNotFoundException {
        synchronized (msgQueue){
            try(RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(msgQueue.getName()),"rw")){
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                //改变读取下标
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.read(bufferSrc);
                //将bufferSrc取出来,从二进制数组转换成类对象
                Message diskMsg = (Message) BinaryTool.fromBytes(bufferSrc);
                if(diskMsg.getIsValid() == 0x1) {
                    //只有当消息有效时才进行删除
                    diskMsg.setIsValid((byte) 0x0);
                    //重新写入消息到文件中
                    byte[] bufferDest = BinaryTool.toByte(diskMsg);
                    randomAccessFile.seek(message.getOffsetBeg());
                    randomAccessFile.write(bufferDest);
                }
            }
        }
        Stat stat = readStat(msgQueue.getName());
        if(stat.validMessageCount > 0) stat.validMessageCount -= 1;
        writeStat(msgQueue.getName(), stat);
    }
    //将文件中的消息加载到内存中
    public List<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        List<Message> messageList = new LinkedList<>();
        try(InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))){
            try(DataInputStream dataInputStream = new DataInputStream(inputStream)){
                //使用这个变量记录当前文件光标
                long currentOffset = 0;
                while (true){
                    //1.读取消息长度
                    int messageSize = dataInputStream.readInt();
                    //2.读取消息
                    byte[] buffer = new byte[messageSize];
                    int actualSize = dataInputStream.read(buffer);
                    if(messageSize != actualSize){
                        throw new MqException("[MessageFileManager]-文件格式错误!queueName="+queueName);
                    }
                    //3.反序列化为对象
                    Message message = (Message) BinaryTool.fromBytes(buffer);
                    //无效数据直接跳过
                    if(message.getIsValid() != 0x1) {
                        currentOffset += (messageSize + 4);
                        continue;
                    }
                    //4.手动计算,填写对象的offsetBeg和offsetEnd
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + messageSize + 4);
                    currentOffset += (messageSize + 4);
                    messageList.add(message);
                }
            }catch (EOFException e){
                //处理文件读到末尾时的异常
                System.out.println("[MessageFileManager]-恢复Message数据完成");
            }
        }
        return messageList;
    }

    //检查是否需要GC,约定当消息数量超过2000条且有效消息数量小于0.5时触发GC
    public boolean checkGC(String queueName){
        Stat stat = readStat(queueName);
        if(stat.totalMessageCount >= 2000 && (double) stat.validMessageCount / (double)stat.totalMessageCount < 0.5){
            return true;
        }
        return false;
    }
    public String getQueueDataNameNewPath(String queueName){
        return getQueuePath(queueName) + "queue_data_new.txt";
    }
    //负责垃圾回收
    //使用复制算法完成,创建一个新文件,将有效消息复制到新文件中,再对旧文件进行删除,将其改名为新文件
    public void gc(MSGQueue msgQueue) throws MqException, IOException, ClassNotFoundException {
        synchronized (msgQueue){
            long gcBeg = System.currentTimeMillis();
            //1.创建一个新文件
            File queueDataNewFile = new File(getQueueDataNameNewPath(msgQueue.getName()));
            if(queueDataNewFile.exists()){
                throw new MqException("[MessageFileManager]-gc时发现该队列的queue_data_new已存在,queueName="+msgQueue.getName());
            }
            boolean ok = queueDataNewFile.createNewFile();
            if(!ok){
                throw new MqException("[MessageFileManager]-gc时创建该队列的queue_data_new失败,queueName="+msgQueue.getName());
            }
            //2.从旧的文件中取出有效消息
            List<Message> messageList = loadAllMessageFromQueue(msgQueue.getName());
            //3.把有效消息放到新的文件中
            try(OutputStream outputStream = new FileOutputStream(queueDataNewFile)) {
                try(DataOutputStream dataOutputStream = new DataOutputStream(outputStream)){
                    for (Message message:messageList){
                        byte[] buffer = BinaryTool.toByte(message);
                        dataOutputStream.writeInt(buffer.length);
                        dataOutputStream.write(buffer);
                    }
                }
            }
            //4.删除旧文件,对新文件改名
            File queueDataOldFile = new File(getQueueDataPath(msgQueue.getName()));
            ok = queueDataOldFile.delete();
            if(!ok){
                throw new MqException("[MessageFileManager]-gc时删除旧文件数据失败,queueName="+msgQueue.getName());
            }
            ok = queueDataNewFile.renameTo(queueDataOldFile);
            if(!ok){
                throw new MqException("[MessageFileManager]-gc时文件重命名失败,queueName="+msgQueue.getName());
            }
            //5.更新统计文件
            Stat stat = readStat(msgQueue.getName());
            stat.totalMessageCount = messageList.size();
            stat.validMessageCount = messageList.size();
            writeStat(msgQueue.getName(),stat);

            long gcEnd = System.currentTimeMillis();
            System.out.println("[MessageFileManager]-gc执行完毕,queuueName="+msgQueue.getName()+"共耗时"+(gcEnd-gcBeg) +"miles");
        }
    }
}
