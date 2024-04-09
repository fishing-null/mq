package com.example.mq;

import com.example.datacenter.MessageFileManager;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();
    private final String queueNameTest1 = "testQueue1";
    private final String queueNameTest2 = "testQueue2";

    //在每个方法执行前调用
    @BeforeEach
    public void setUp() throws IOException {
        //创建两个队列用于测试
        messageFileManager.createQueueDir(queueNameTest1);
        messageFileManager.createQueueDir(queueNameTest2);

    }


    //在每个方法执行后调用
    @AfterEach
    public void teatDown() throws IOException {
        //将创建的两个队列销毁
        messageFileManager.destroyQueueDir(queueNameTest1);
        messageFileManager.destroyQueueDir(queueNameTest2);
    }
    @Test
    //验证文件是否创建成功,queue_data.txt & queue_stats.txt是否存在
    public void testCreateFiles(){
        //构造路径
        File queueDataFile1 = new File("./data/" + queueNameTest1 + "/queue_data.txt");
        Assertions.assertEquals(true,queueDataFile1.isFile());
        File queueStatFile1 = new File("./data/" + queueNameTest1 + "/queue_stat.txt");
        Assertions.assertEquals(true,queueStatFile1.isFile());
        File queueDataFile2 = new File("./data/" + queueNameTest2 + "/queue_data.txt");
        Assertions.assertEquals(true,queueDataFile1.isFile());
        File queueStatFile2 = new File("./data/" + queueNameTest2 + "/queue_stat.txt");
        Assertions.assertEquals(true,queueDataFile1.isFile());
    }
    @Test
    //测试readStat()&writeStat()
    public void testReadWriteStat(){
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalMessageCount = 100;
        stat.validMessageCount = 50;
        //通过反射调用MessageFileManager的readStat方法
        ReflectionTestUtils.invokeMethod(messageFileManager,"writeStat",queueNameTest1,stat);
        //通过反射调用MessageFileManager的writeStat方法
        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(messageFileManager,"readStat",queueNameTest1);
        Assertions.assertEquals(100,newStat.totalMessageCount);
        Assertions.assertEquals(50,newStat.validMessageCount);
    }
    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        //构造队列和消息用于测试
        Message message = createTestMessage("testMessage");
        MSGQueue msgQueue = createTestQueue();

        //调用发送消息方法
        messageFileManager.sendMessage(msgQueue,message);
        //验证stat文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager,"readStat",queueNameTest1);
        Assertions.assertEquals(1,stat.validMessageCount);
        Assertions.assertEquals(1,stat.totalMessageCount);
        //验证data文件
        List<Message> messageList = messageFileManager.loadAllMessageFromQueue(queueNameTest1);
        Assertions.assertEquals(1,messageList.size());
        Message curMessage = messageList.get(0);
        Assertions.assertEquals(message.getMessageId(),curMessage.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(),curMessage.getRoutingKey());
        Assertions.assertEquals(message.getDeliverMode(),curMessage.getDeliverMode());
        Assertions.assertArrayEquals(message.getBody(),curMessage.getBody());
        System.out.println("message:" + curMessage);
    }

    @Test
    //测试从队列冲加载数据
    public void testLoadAllMessageFromQueue() throws IOException, MqException, ClassNotFoundException {
        //向队列中插入100条数据
        MSGQueue msgQueue = createTestQueue();
        List<Message> expectedMessageList = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(msgQueue,message);
            expectedMessageList.add(message);
        }
        List<Message> actualMessageList = messageFileManager.loadAllMessageFromQueue(queueNameTest1);
        Assertions.assertEquals(expectedMessageList.size(),actualMessageList.size());
        //对比内容是否正确
        for (int i = 0; i < actualMessageList.size(); i++) {
            Message message = expectedMessageList.get(i);
            Message curMessage = actualMessageList.get(i);
            System.out.println("[" + i + "] actualMessage=" + curMessage);
            System.out.println("[" + i + "] message=" + message);
            Assertions.assertEquals(message.getMessageId(),curMessage.getMessageId());
            Assertions.assertEquals(message.getRoutingKey(),curMessage.getRoutingKey());
            Assertions.assertEquals(message.getDeliverMode(),curMessage.getDeliverMode());
            Assertions.assertArrayEquals(message.getBody(),curMessage.getBody());
            Assertions.assertEquals(0x1,curMessage.getIsValid());
            Assertions.assertEquals(0x1,message.getIsValid());
        }
    }
    @Test
    public void testDeleteMessage() throws IOException, ClassNotFoundException, MqException {
        //向队列插入十条消息,再随便删除几条
        MSGQueue msgQueue = createTestQueue();
        List<Message> expectedMessageList = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(msgQueue,message);
            expectedMessageList.add(message);
        }
        //删除三条消息
        messageFileManager.deleteMessage(msgQueue,expectedMessageList.get(9));
        messageFileManager.deleteMessage(msgQueue,expectedMessageList.get(8));
        messageFileManager.deleteMessage(msgQueue,expectedMessageList.get(7));
        List<Message> actualMessageList = messageFileManager.loadAllMessageFromQueue(queueNameTest1);
        Assertions.assertEquals(7,actualMessageList.size());
        //对比内容是否正确
        for (int i = 0; i < actualMessageList.size(); i++) {
            Message message = expectedMessageList.get(i);
            Message curMessage = actualMessageList.get(i);
            System.out.println("[" + i + "] actualMessage=" + curMessage);
            System.out.println("[" + i + "] message=" + message);
            Assertions.assertEquals(message.getMessageId(),curMessage.getMessageId());
            Assertions.assertEquals(message.getRoutingKey(),curMessage.getRoutingKey());
            Assertions.assertEquals(message.getDeliverMode(),curMessage.getDeliverMode());
            Assertions.assertArrayEquals(message.getBody(),curMessage.getBody());
            Assertions.assertEquals(0x1,curMessage.getIsValid());
            Assertions.assertEquals(0x1,message.getIsValid());
        }
    }
    @Test
    public void testGC() throws IOException, MqException, ClassNotFoundException {
        //添加100条消息,再删除一半消息,比较删除前后文件大小
        MSGQueue msgQueue = createTestQueue();
        List<Message> expectedMessageList = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(msgQueue,message);
            expectedMessageList.add(message);
        }
        File beforeGCFile = new File("./data/" + queueNameTest1 + "/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();
        //对下标为偶数的消息进行删除
        for (int i = 0; i < 100; i += 2) {
            messageFileManager.deleteMessage(msgQueue,expectedMessageList.get(i));
        }
        messageFileManager.gc(msgQueue);
        //重新读取文件,验证文件和之前内容是否匹配
        List<Message> actualMessageList = messageFileManager.loadAllMessageFromQueue(queueNameTest1);
        Assertions.assertEquals(50,actualMessageList.size());
        for (int i = 0; i < actualMessageList.size(); i++) {
            Message message = expectedMessageList.get(2*i + 1);
            Message curMessage = actualMessageList.get(i);
            System.out.println("[" + i + "] actualMessage=" + curMessage);
            System.out.println("[" + i + "] message=" + message);
            Assertions.assertEquals(message.getMessageId(),curMessage.getMessageId());
            Assertions.assertEquals(message.getRoutingKey(),curMessage.getRoutingKey());
            Assertions.assertEquals(message.getDeliverMode(),curMessage.getDeliverMode());
            Assertions.assertArrayEquals(message.getBody(),curMessage.getBody());
            Assertions.assertEquals(0x1,curMessage.getIsValid());
            Assertions.assertEquals(0x1,message.getIsValid());
        }
        File afterGCFile = new File("./data/" + queueNameTest1 + "/queue_data.txt");
        long afterGCLength = afterGCFile.length();
        System.out.println("before: "+ beforeGCLength);
        System.out.println("after: "+ afterGCLength);
        Assertions.assertTrue(beforeGCLength > afterGCLength);

    }
    private MSGQueue createTestQueue() {
        MSGQueue msgQueue = new MSGQueue();
        msgQueue.setDurable(true);
        msgQueue.setName(queueNameTest1);
        msgQueue.setExclusive(true);
        return msgQueue;
    }

    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey",null,content.getBytes());
        return message;
    }
}
