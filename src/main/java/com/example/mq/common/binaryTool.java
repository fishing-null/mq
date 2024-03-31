package com.example.mq.common;

import java.io.*;

//工具类 存储的都是一些静态方法
public class binaryTool {
    //把一个对象序列化成数组
    public static byte[] toByte(Object object) throws IOException {
        //这个流对象相当于一个变长的字节数组
        //可以将object序列化的数据写入到byteArrayOutputStream中,然后再统一转换成byte[]
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectoutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                //此处进行序列化
                objectoutputStream.writeObject(object);
            }
            //将byteArrayOutputStream中的二进制数据取出来转换成byte[]
            return byteArrayOutputStream.toByteArray();
        }
    }
    //把一个字节数组反序列化成一个对象
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        Object object = null;
        try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)){
            try(ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)){
                object = objectInputStream.readObject();
            }
        }
        return object;
    }
}
