package com.example.mq.mqserver;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.datacenter.DiskDataCenter;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;

/**
 * 虚拟主机,每个虚拟主机管理独自的交换机、队列、绑定、消息、各个虚拟主机间互不干扰
 * 同时提供部分api供上层调用
 */
public class VirtualHost {
    private String virtualHostName;
    private MemoryDataCenter memoryDataCenter;
    private DiskDataCenter diskDataCenter;
    public VirtualHost(String name){
        this.virtualHostName = name;
        diskDataCenter.init();
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | ClassNotFoundException | MqException e) {
            e.printStackTrace();
            System.out.println("[VirtualHost]恢复内存数据失败!");
        }
    }
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete, Map<String,Object> arguments){
         exchangeName = virtualHostName+exchangeName;
         try {
             if(memoryDataCenter.getExchange(exchangeName) != null){
                 System.out.println("[VirtualHost]-Exchange"+exchangeName+"已经存在!");
                return true;
             }
             Exchange exchange = new Exchange();
             exchange.setName(exchangeName);
             exchange.setType(exchangeType);
             exchange.setAutoDelete(autoDelete);
             exchange.setDurable(durable);
             exchange.setArguments(arguments);
             if(durable){
                 diskDataCenter.insertExchange(exchange);
             }
             memoryDataCenter.insertExchange(exchange);
             System.out.println("[VirtualHost]交换机创建完成!exchangeName="+exchangeName);
             return true;
         }catch (Exception e){
             System.out.println("[VirtualHost]交换机创建失败!exchangeName="+exchangeName);
             e.printStackTrace();
             return false;
         }
    }
    public boolean exchangeDelete(String exchangeName){
        exchangeName = virtualHostName+exchangeName;
        try{
            Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
            if(toDelete == null){
                throw new MqException("[VirtualHost]交换机不存在,删除失败!exchangeName="+exchangeName);
            }
            if(toDelete.isDurable()){
                diskDataCenter.deleteExchange(exchangeName);
            }
            memoryDataCenter.deleteExchange(exchangeName);
            System.out.println("[VirtualHost]交换机删除成功!exchangeName="+exchangeName);
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]交换机删除失败!exchangeName="+exchangeName);
            e.printStackTrace();
            return false;
        }
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }
}
