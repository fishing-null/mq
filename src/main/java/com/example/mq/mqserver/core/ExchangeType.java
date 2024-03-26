package com.example.mq.mqserver.core;

public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);
    private int id;

    ExchangeType(int id) {
        this.id = id;
    }
}
