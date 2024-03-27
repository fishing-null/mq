package com.example.datacenter;

public class MessageFileManager {
    public static class Stat{
        public String totalMessageCount;
        public String validMessageCount;
    }
    private String getQueuePath(String queueName){
        return "./data/" + queueName;
    }
    private String getMessageDataPath(String queueName){
        return getQueuePath(queueName) + "message_data.txt";
    }

    private String getMessageStatsPath(String queueName){
        return getQueuePath(queueName) + "message_stats.txt";
    }
}
