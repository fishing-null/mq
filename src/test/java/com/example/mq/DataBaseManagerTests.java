package com.example.mq;

import com.example.mq.mqserver.datacenter.DataBaseManager;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;


import java.util.List;


@SpringBootTest
public class DataBaseManagerTests {
    private DataBaseManager dataBaseManager = new DataBaseManager();
    //每个测试方法调用前,都要执行该方法进行数据库初始化操作
    @BeforeEach
    public void setUp(){
        MqApplication.context = SpringApplication.run(MqApplication.class);
        dataBaseManager.init();
    }
    //每个测试方法调用后,都要执行该方法进行数据库清除操作
    @AfterEach
    public void tearDown(){
        MqApplication.context.close();
        dataBaseManager.deleteDB();
    }
    @Test
    public void testInitTable(){
        //查询表中所有数据,测试结果是否符合预期
        //exchangeList中期望得到一条数据,name字段为空字符串,exchangeType字段为DIRECT
        //msgQueueList&bindingList中没有数据
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        List<MSGQueue> msgQueueList = dataBaseManager.selectAllQueues();
        List<Binding> bindingList = dataBaseManager.selectAllEBindings();

        Assertions.assertEquals(1,exchangeList.size());
        Assertions.assertEquals("",exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT,exchangeList.get(0).getType());
        Assertions.assertEquals(0,msgQueueList.size());
        Assertions.assertEquals(0,bindingList.size());
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
    public void testCreateExchange(){
        Exchange exchange = createTestExchange("testExchange");
        dataBaseManager.insertExchange(exchange);
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        Exchange exchange1 = exchangeList.get(1);
        Assertions.assertEquals(2,exchangeList.size());
        Assertions.assertEquals("testExchange",exchange1.getName());
        Assertions.assertEquals("",exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT,exchangeList.get(0).getType());
        Assertions.assertEquals(ExchangeType.FANOUT,exchange1.getType());
        Assertions.assertEquals(1,exchange1.getArguments("aaa"));
        Assertions.assertEquals(2,exchange1.getArguments("bbb"));
        Assertions.assertEquals(false,exchange1.isAutoDelete());
        Assertions.assertEquals(true,exchange1.isDurable());

    }
    @Test
    public void deleteExchange(){
        //构造交换机插入数据库,再按照名字删除即可
        Exchange exchange = createTestExchange("testExchange");
        dataBaseManager.insertExchange(exchange);
        List<Exchange> exchangeList = dataBaseManager.selectAllExchanges();
        Assertions.assertEquals(2,exchangeList.get(1).getName());
        //进行删除操作 再次查询
        dataBaseManager.deleteExchange("testExchange");
        exchangeList=dataBaseManager.selectAllExchanges();
        Assertions.assertEquals(1,exchangeList.size());
        Assertions.assertEquals(".",exchangeList.get(0).getName());
    }
}
