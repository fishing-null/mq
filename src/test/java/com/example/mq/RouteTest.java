package com.example.mq;

import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.Message;
import com.example.mq.mqserver.core.Router;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RouteTest {
    private Router router = new Router();
    private Binding binding = null;
    private Message message = null;
    @BeforeEach
    public void setUp(){
         binding = new Binding();
         message= new Message();
    }
    @AfterEach
    public void teatDown(){
        binding = null;
        message = null;
    }
    /*
    提供几组测试用例看结果是否符合预期
    bindingKey          routingKey             result

     */
    @Test
    public void test1() throws Exception {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test2() throws Exception {
        binding.setBindingKey("aaa");
        message.setRoutingKey("aaa");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test4() throws Exception {
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test5() throws Exception {
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test6() throws Exception {
        binding.setBindingKey("aaa.bbb.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test7() throws Exception {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test8() throws Exception {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test9() throws Exception {
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test10() throws Exception {
        binding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test11() throws Exception {
        binding.setBindingKey("*.aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test12() throws Exception {
        binding.setBindingKey("#");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test13() throws Exception {
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test14() throws Exception {
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.bbb.ddd.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test15() throws Exception {
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test16() throws Exception {
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

}
