package com.example.mq.common;

public class MqException extends Exception{
    public MqException(String reason) {
        super(reason);
    }
}
