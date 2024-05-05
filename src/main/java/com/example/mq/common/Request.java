package com.example.mq.common;

/*
 * 表示请求的格式
 * type表示请求的类型
 * length表示请求的长度
 * payload根据请求类型以及type有不同的取值
 */
public class Request {
    private int type;
    private int length;
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
