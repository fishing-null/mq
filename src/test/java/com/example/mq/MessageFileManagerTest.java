package com.example.mq;

import com.example.datacenter.MessageFileManager;
import com.example.mq.mqserver.core.MSGQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

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
}
