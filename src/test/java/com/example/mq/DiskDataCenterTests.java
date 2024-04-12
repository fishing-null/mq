package com.example.mq;

import com.example.mq.datacenter.DataBaseManager;
import com.example.mq.datacenter.DiskDataCenter;
import com.example.mq.datacenter.MessageFileManager;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
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
}
