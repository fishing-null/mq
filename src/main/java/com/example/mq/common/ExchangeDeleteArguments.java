package com.example.mq.common;

import java.io.Serializable;

public class ExchangeDeleteArguments extends BasicArguments implements Serializable {
    private String exchangeName;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
