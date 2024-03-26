package com.example.mq.mqserver.mapper;

import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Queue;

@Mapper

public interface MetaMapper {
    //三个建表方法
    void createExchangeTable();
    void createBindingTable();
    void createQueueTable();

    //围绕上面三个表的增加和删除操作
    void insertExchange(Exchange exchange);
    void deleteExchange(String exchangeName);
    List<Exchange> selectAllExchanges();
    void insertQueue(MSGQueue msgQueue);
    void deleteQueue(String queueName);
    List<MSGQueue> selectAllQueues();
    void insertBinding(Binding binding);
    void deleteBinding(Binding binding);
    List<Binding> selectAllBindings();
}
