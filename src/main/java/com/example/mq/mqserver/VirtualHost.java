package com.example.mq.mqserver;

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.*;
import com.example.mq.mqserver.datacenter.DiskDataCenter;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 虚拟主机,每个虚拟主机管理独自的交换机、队列、绑定、消息、各个虚拟主机间互不干扰
 * 同时提供部分api供上层调用
 */
public class VirtualHost {
    private String virtualHostName;
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private Router router = new Router();

    private final Object exchangeLocker = new Object();
    private final Object queueLocker = new Object();
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
             synchronized (exchangeLocker){
                 if(memoryDataCenter.getExchange(exchangeName) != null){
                     System.out.println("[VirtualHost]交换机已存在,exhcangeName="+exchangeName);
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
             }
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
           synchronized (exchangeLocker){
               Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
               if(toDelete == null){
                   throw new MqException("[VirtualHost]交换机不存在,删除失败!exchangeName="+exchangeName);
               }
               if(toDelete.isDurable()){
                   diskDataCenter.deleteExchange(exchangeName);
               }
               memoryDataCenter.deleteExchange(exchangeName);
               System.out.println("[VirtualHost]交换机删除成功!exchangeName="+exchangeName);
           }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]交换机删除失败!exchangeName="+exchangeName);
            e.printStackTrace();
            return false;
        }
    }

    public boolean msgQueueDeclare(String queueName,boolean autoDelete,boolean durable,boolean exclusive,Map<String,Object> arguments){
        queueName = virtualHostName+queueName;
        try {
            synchronized (queueLocker){
                //1.查询队列是否存在,存在则直接返回true
                if(memoryDataCenter.getExchange(queueName) != null){
                    System.out.println("[VirtualHost]消息队列已经存在!msgQueueName="+queueName);
                    return true;
                }
                //2.队列不存在,创建队列
                MSGQueue msgQueue = new MSGQueue();
                msgQueue.setName(queueName);
                msgQueue.setAutoDelete(autoDelete);
                msgQueue.setDurable(durable);
                msgQueue.setExclusive(exclusive);
                //3.是否持久化
                if(durable){
                    diskDataCenter.insertMsgQueue(msgQueue);
                }
                memoryDataCenter.insertMsgQueue(msgQueue);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]消息队列创建失败!msgQueueName="+queueName);
            e.printStackTrace();
            return false;
        }
    }
    public boolean msgQueueDelete(String queueName){
        queueName = virtualHostName+queueName;
        try{
            synchronized (queueLocker){
                //1.先找到该队列
                MSGQueue toDelete = memoryDataCenter.getMsgQueue(queueName);
                //2.判断队列存在
                if(toDelete == null){
                    System.out.println("[VirtualHost]消息队列不存在,删除失败!msgQueueName="+queueName);
                }
                //3.硬盘删除队列
                if(toDelete.isDurable()){
                    diskDataCenter.deleteMsgQueue(queueName);
                }
                //4.内存删除队列
                memoryDataCenter.deleteMsgQueue(queueName);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]消息队列删除失败!msgQueueName="+queueName);
            e.printStackTrace();
            return false;
        }
    }
    public boolean queueBind(String exchangeName,String queueName,String bindingKey){
        exchangeName = virtualHostName+exchangeName;
        queueName = virtualHostName+queueName;
        try {
            synchronized (exchangeLocker){
                synchronized (queueLocker){
                    //1.判断binding是否存在
                    Binding bindingIfExist = memoryDataCenter.getBinding(exchangeName,queueName);
                    if(bindingIfExist != null){
                        throw new MqException("[VirtualHost]binding已经存在!queueName="+queueName+"exchangeName="+exchangeName);
                    }
                    //2.验证bindingKey是否合法
                    if(!router.checkBindingKey(bindingKey)){
                        throw new MqException("[VirtualHost]bindingKey非法!bindingKey="+bindingKey);
                    }
                    //3.创建binding对象
                    Binding binding = new Binding();
                    binding.setQueueName(queueName);
                    binding.setExchangeName(exchangeName);
                    binding.setBindingKey(bindingKey);
                    //4.对应的交换机和队列是否存在
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    MSGQueue msgQueue = memoryDataCenter.getMsgQueue(queueName);
                    if(msgQueue == null || exchange == null){
                        throw new MqException("[VirtualHost]exchange不存在或msgQueue不存在!exchangeName="+exchangeName+"queueName="+queueName);
                    }
                    //5.是否持久化
                    if(exchange.isDurable() && msgQueue.isDurable()){
                        diskDataCenter.insertBinding(binding);
                    }
                    memoryDataCenter.insertBinding(binding);
                    System.out.println("[VirtualHost]binding创建成功!exchangeName="+exchangeName+"queueName="+queueName);
                }
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]binding创建失败!exchangeName="+exchangeName+"queueName="+queueName);
            e.printStackTrace();
            return false;
        }
    }
    public boolean queueUnbind(String exchangeName,String queueName){
        exchangeName = virtualHostName + exchangeName;
        queueName = virtualHostName + queueName;
        try {
            synchronized (exchangeLocker){
                synchronized (queueLocker){
                    Binding binding = memoryDataCenter.getBinding(exchangeName,queueName);
                    if(binding == null){
                        throw new MqException("[VirtualHost]binding删除失败!exchangeName="+exchangeName+"queueName="+queueName);
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    MSGQueue msgQueue = memoryDataCenter.getMsgQueue(queueName);
                    if(exchange == null || msgQueue == null){
                        throw new MqException("[VirtualHost]exchange不存在或msgQueue不存在!exchangeName="+exchangeName+"queueName="+queueName);
                    }
                    diskDataCenter.deleteBinding(binding);
                    memoryDataCenter.deleteBinding(binding);
                    System.out.println("[VirtualHost]binding删除成功!");
                }
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost]binding删除失败!exchangeName="+exchangeName+"queueName="+queueName);
            e.printStackTrace();
            return false;
        }
    }
    //构建消息转发到对应的交换机中
    public boolean basicPublish(String exchangeName,String routingKey,BasicProperties basicProperties,byte[] body){
        try {
            exchangeName = virtualHostName+exchangeName;
            //1.验证routingKey是否合法
            if(!router.checkRoutingKey(routingKey)){
                throw new MqException("[VirtualHost]routingKey不合法!routingKey="+routingKey);
            }
            //2.验证交换机是否存在
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if(exchange == null){
                throw new MqException("[VirtualHost]交换机不存在!exchangeName="+exchangeName);
            }
            //3.判断交换机类型
            if(exchange.getType() == ExchangeType.DIRECT){
                //直接交换机,采用queueName当作routingKey
                String queueName = virtualHostName + routingKey;
                //构建队列
                MSGQueue msgQueue = memoryDataCenter.getMsgQueue(queueName);
                if(msgQueue == null){
                    throw new MqException("[VirtualHost]队列不存在!queueName="+queueName);
                }
                //构建消息
                Message message = Message.createMessageWithId(routingKey,basicProperties,body);
                sendMessage(msgQueue,message);
            }else {
                //按照fanout和topic来转发
                ConcurrentHashMap<String,Binding> bindingsMap = memoryDataCenter.getAllBindings(exchangeName);
                for(Map.Entry<String,Binding> entry:bindingsMap.entrySet()){
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getMsgQueue(binding.getQueueName());
                    if(queue == null){
                        System.out.println("[VirtualHost]队列不存在!queueName="+queue.getName());
                        continue;
                    }
                    Message message = Message.createMessageWithId(routingKey,basicProperties,body);
                    //判定消息能否转发给i队列
                    if(!router.route(exchange.getType(),binding,message)){
                        System.out.println("[VirtualHost]转发规则不匹配!routingKey="+routingKey);
                        continue;
                    }
                    //转发消息给队列
                    sendMessage(queue,message);
                }
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    private void sendMessage(MSGQueue msgQueue, Message message) {

    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }
}
