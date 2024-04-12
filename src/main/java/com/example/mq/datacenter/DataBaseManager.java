package com.example.mq.datacenter;

import com.example.mq.MqApplication;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.mapper.MetaMapper;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.List;

public class DataBaseManager {
    private MetaMapper metaMapper;
    //针对数据库进行初始化 若数据库不存在则建表插入默认数据,存在则什么都不做
    public void init(){
        metaMapper = MqApplication.context.getBean(MetaMapper.class);
        if(!checkDBExist()){
            File dataDir = new File("./data");
            //先创建dir目录
            dataDir.mkdirs();
            createTable();
            createDefaultValue();
            System.out.println("[DataBaseManager]-数据库初始化完成!!");
        }else {
            System.out.println("[DataBaseManager]-数据库已存在!");
        }

    }

    //创建一个匿名的DIRECT交换机
    public void createDefaultValue() {
        Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
        System.out.println("[DataBaseManager]-创建初始数据完成");
    }

    public void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
    }

    public void deleteDB(){
        File file = new File("./data/meta.db");
        boolean ret = file.delete();
        if(ret){
            System.out.println("[DataBaseManager]-数据库文件删除成功!");
        }else {
            System.out.println("[DataBaseManager]-删除数据库文件失败!");
        }
        File dataDir = new File("./data");
        dataDir.delete();
        if(ret){
            System.out.println("[DataBaseManager]-数据库目录删除成功!");
        }else {
            System.out.println("[DataBaseManager]-删除数据库目录失败!");
        }
    }
    private boolean checkDBExist() {
        File file = new File("./data/meta.db");
        if(file.exists()){
            return true;
        }
        return false;
    }

    public List<Exchange> selectAllExchanges(){
        return metaMapper.selectAllExchanges();
    }
    public List<MSGQueue> selectAllQueues(){
        return metaMapper.selectAllQueues();
    }

    public List<Binding> selectAllEBindings(){
        return metaMapper.selectAllBindings();
    }

    public void insertExchange(Exchange exchange){
        metaMapper.insertExchange(exchange);
    }
    public void insertQueue(MSGQueue queue){
        metaMapper.insertQueue(queue);
    }
    public void insertBinding(Binding binding){
        metaMapper.insertBinding(binding);
    }

    public void deleteExchange(String exchangeName){
        metaMapper.deleteExchange(exchangeName);
    }
    public void deleteQueue(String queueName){
        metaMapper.deleteQueue(queueName);
    }
    public void deleteBinding(Binding binding){
        metaMapper.deleteBinding(binding);
    }
}
