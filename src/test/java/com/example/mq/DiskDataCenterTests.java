package com.example.mq;

import com.example.mq.common.MqException;
import com.example.mq.datacenter.DataBaseManager;
import com.example.mq.datacenter.DiskDataCenter;
import com.example.mq.datacenter.MessageFileManager;
import com.example.mq.mqserver.core.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
@SpringBootTest
public class DiskDataCenterTests {
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private DataBaseManager dataBaseManager = new DataBaseManager();
    private MessageFileManager messageFileManager = new MessageFileManager();
    private final String testQueueName = "testQueueName";
    @BeforeEach
    public void setUp() throws IOException {
        //在每个测试方法执行前执行初始化操作,创建数据库,创建队列文件
        MqApplication.context = SpringApplication.run(MqApplication.class);
        diskDataCenter.init();
        messageFileManager.createQueueDir(testQueueName);
    }
    @AfterEach
    public void tearDown() throws IOException {
        //在每个测试方法执行后执行删除操作.清除数据库,清除队列文件
        messageFileManager.destroyQueueDir(testQueueName);
        //此处不close的话无法删除文件
        MqApplication.context.close();
        dataBaseManager.deleteDB();
    }
    private Exchange createTestExchange(String exchangeName){
        Exchange exchange = new Exchange();
        //创建一个交换机用于测试
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.FANOUT);
        exchange.setDurable(true);
        exchange.setArguments("aaa",1);
        exchange.setArguments("bbb",2);
        exchange.setAutoDelete(false);
        return exchange;
    }
    @Test
    public void testExchangeModule(){
        //测试插入交换机和查询交换机,创建20个交换机插入数据库,
        // 创建库表时自带一条交换机数据,一共21
        // 再删除一条交换机数据,一共20条数据
        List<Exchange> exchangeList = new LinkedList<>();
        for (int i = 0; i < 20; i++) {
            Exchange exchange = createTestExchange("testExchange"+ i );
            diskDataCenter.insertExchange(exchange);

        }
        exchangeList = diskDataCenter.selectAllExchanges();
        Assertions.assertEquals(21,exchangeList.size());
        diskDataCenter.deleteExchange("testExchange0");
        //删除一条,此时剩20条
        exchangeList = diskDataCenter.selectAllExchanges();
        Assertions.assertEquals(20,exchangeList.size());
    }

    @Test
    public void testMessageModule() throws IOException, MqException, ClassNotFoundException {
        //测试插入消息,删除消息,从队列中加载消息
        List<Message> expectedMessageList= new LinkedList<>();
        MSGQueue msgQueue = createTestQueue();
        //向队列中插入20条消息,再随便删几条
        for (int i = 0; i < 20; i++) {
            Message message = createTestMessage("testMessage" + i);
            diskDataCenter.sendMessage(msgQueue,message);
            expectedMessageList.add(message);
        }
        //删除前20,删除后16
        Assertions.assertEquals(20,expectedMessageList.size());
        diskDataCenter.deleteMessage(msgQueue,expectedMessageList.get(4));
        diskDataCenter.deleteMessage(msgQueue,expectedMessageList.get(5));
        diskDataCenter.deleteMessage(msgQueue,expectedMessageList.get(6));
        diskDataCenter.deleteMessage(msgQueue,expectedMessageList.get(7));
        expectedMessageList = diskDataCenter.loadAllMessagesFromQueue(testQueueName);
        Assertions.assertEquals(16,expectedMessageList.size());

        //测试从文件加载消息
        List<Message> actualMessageList= diskDataCenter.loadAllMessagesFromQueue(testQueueName);
        Assertions.assertEquals(expectedMessageList.size(),actualMessageList.size());

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
    public void testBindingModule(){
        List<Binding> bindingList = new LinkedList<>();
        for (int i = 0; i < 20; i++) {
            Binding binding = createTestBinding(i);
            diskDataCenter.insertBinding(binding);
        }
        bindingList = diskDataCenter.selectAllBindings();
        Assertions.assertEquals(20,bindingList.size());
        diskDataCenter.deleteBinding(bindingList.get(0));
        diskDataCenter.deleteBinding(bindingList.get(1));
        bindingList = diskDataCenter.selectAllBindings();
        Assertions.assertEquals(18,bindingList.size());
    }

    private Binding createTestBinding(int keyId) {
        Binding binding = new Binding();
        binding.setBindingKey("testBindingkey"+keyId);
        binding.setExchangeName("testExchange"+keyId);
        binding.setQueueName(testQueueName+keyId);
        return binding;
    }

    private MSGQueue createTestQueue() {
        MSGQueue msgQueue = new MSGQueue();
        msgQueue.setDurable(true);
        msgQueue.setName(testQueueName);
        msgQueue.setExclusive(true);
        return msgQueue;
    }

    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey",null,content.getBytes());
        return message;
    }
}
