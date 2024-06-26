package com.example.mq.common;

import java.io.Serializable;

/*
 *这个类表示各个远程通用的方法的参数的公共信息
 */
public class BasicArguments implements Serializable {
    //匹配一组对应的请求和响应
    protected String rid;
    //这次通信使用的channel标识
    protected String channelId;

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
}
