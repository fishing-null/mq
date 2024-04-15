package com.example.mq;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.*;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootTest
public class MemoryDataCenterTest {
    private MemoryDataCenter memoryDataCenter = null;
    @BeforeEach
    public void setUp(){
        memoryDataCenter = new MemoryDataCenter();
    }
    @AfterEach
    public void tearDown(){
        memoryDataCenter = null;
    }

    private Exchange createTestExchange(String exchangeName){
        Exchange testExchange = new Exchange();
        testExchange.setAutoDelete(false);
        testExchange.setName(exchangeName);
        testExchange.setType(ExchangeType.FANOUT);
        testExchange.setDurable(true);
        return testExchange;
    }
    private MSGQueue createTestQueue(String queueName){
        MSGQueue testMsgQueue = new MSGQueue();
        testMsgQueue.setName(queueName);
        testMsgQueue.setExclusive(true);
        testMsgQueue.setDurable(true);
        testMsgQueue.setAutoDelete(false);
        return testMsgQueue;
    }

    @Test
    public void testExchange() throws MqException {
        //1.创建一个交换机,进行插入
        Exchange expectedExchange = createTestExchange("testExchange");
        memoryDataCenter.insertExchange(expectedExchange);
        Exchange actualExchange = memoryDataCenter.getExchange("testExchange");
        Assertions.assertEquals(expectedExchange,actualExchange);
        //2.对交换机进行删除
        memoryDataCenter.deleteExchange("testExchange");
        actualExchange = memoryDataCenter.getExchange("testExchange");
        //3.查询交换机是否存在
        Assertions.assertNull(actualExchange);
    }
    @Test
    public void testMsgQueue() throws MqException {
        MSGQueue expectedMsgQueue = createTestQueue("testQueue");
        memoryDataCenter.insertMsgQueue(expectedMsgQueue);
        MSGQueue actualMsgQueue = memoryDataCenter.getMsgQueue("testQueue");
        Assertions.assertEquals(expectedMsgQueue,actualMsgQueue);
        memoryDataCenter.deleteMsgQueue("testQueue");
        actualMsgQueue = memoryDataCenter.getMsgQueue("testQueue");
        Assertions.assertNull(actualMsgQueue);
    }
    @Test
    public void testBinding() throws MqException {
        Binding expectedBinding = new Binding();
        expectedBinding.setBindingKey("testBindingKey");
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        memoryDataCenter.insertBinding(expectedBinding);
        Binding actualBinding = memoryDataCenter.getBinding("testExchange","testQueue");
        Assertions.assertEquals(expectedBinding,actualBinding);
        ConcurrentHashMap<String,Binding> bindingMap = memoryDataCenter.getAllBindings("testExchange");
        Assertions.assertEquals(1,bindingMap.size());
        Assertions.assertEquals(expectedBinding,bindingMap.get("testQueue"));
        memoryDataCenter.deleteBinding(expectedBinding);
        actualBinding = memoryDataCenter.getBinding("testExchange","testQueue");
        Assertions.assertNull(actualBinding);
    }
    @Test
    public void testMessage() throws MqException {
        Message expectedMessage = createTestMessage("testMessage");
        memoryDataCenter.insertMessage(expectedMessage);
        Message acutalMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,acutalMessage);
        memoryDataCenter.deleteMessage(expectedMessage.getMessageId());
        acutalMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertNull(acutalMessage);
    }

    private Message createTestMessage(String content) {
        Message testMessage = Message.createMessageWithId("testRoutingKey",null,content.getBytes());
        return testMessage;
    }
    @Test
    public void testSendMessage(){
        //插入10条消息到队列中
        MSGQueue msgQueue = createTestQueue("testQueue");
        List<Message> messageList = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            memoryDataCenter.sendMessage(msgQueue,message);
            messageList.add(message);
        }
        List<Message> actualMessageList = new LinkedList<>();
        //从队列中取出消息
        while (true){
            Message message = memoryDataCenter.pollMessage("testQueue");
            if(message == null){
                break;
            }
            actualMessageList.add(message);
        }
        //比较每条消息是否相同
        Assertions.assertEquals(messageList.size(),actualMessageList.size());
        for (int i = 0; i < actualMessageList.size(); i++) {
            Assertions.assertEquals(messageList.get(i),actualMessageList.get(i));
        }
    }
    @Test
    public void testMessageWaitAck() throws MqException {
        Message expectedMessage = createTestMessage("testMessage");
        memoryDataCenter.insertMessageWaitAck("testQueue",expectedMessage);
        Message actualMessage = memoryDataCenter.getMessageWaitAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,actualMessage);
        memoryDataCenter.deleteMessageWaitAck("testQueue",expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.getMessageWaitAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }
}
