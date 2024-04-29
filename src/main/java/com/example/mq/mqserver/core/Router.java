package com.example.mq.mqserver.core;

public class Router {
    /*
     bindingKey合法性检验
     bindingKey只能由数字、字母、下划线、点号、星号以及井号组成
     */
    public boolean checkBindingKey(String bindingKey){
        //TODO
        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);
            if(ch >= 'A' && ch <= 'Z'){
                continue;
            }
            if(ch >= 'a' && ch <='z'){
                continue;
            }
            if(ch >= '0' && ch <= '9'){
                continue;
            }
            if(ch == '_' || ch == '.' || ch == '#' || ch == '*'){
                continue;
            }
            return false;
        }
        //检验是否符合规则*和#只能作为单个的字符出现
        //例如aaa.*.bbb || aaa.#.bbb
        //像aaa.a*.bbb是不被允许的
        String[] words = bindingKey.split("//.");
        for(String word:words){
            if(word.length() > 1 && (word.contains("#") || word.contains("*"))){
                return false;
            }
        }
        //人为约定一些bindingKey规则
        //aaa.*.*.bbb 允许
        //aaa.#.#.bbb 不允许
        //aaa.#.*.bbb 不允许
        //aaa.*.#.bbb 不允许
        for (int i = 0; i < words.length-1; i++) {
            if(words[i].equals("*") && words[i+1].equals("#")){
                return false;
            }
            if(words[i].equals("#") && words[i+1].equals("#")){
                return false;
            }
            if(words[i].equals("#") && words[i+1].equals("*")){
                return false;
            }
        }
        return true;
    }
    /*
    routingKey合法性检验
    routingKey只能由数字、字母、下划线和点号组成
     */
    public boolean checkRoutingKey(String routingKey){
        if(routingKey.length() == 0){
            //routingKey为空,但是是合法的,如扇出交换机
            return true;
        }
        for (int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            if(ch >= 'A' && ch <= 'Z'){
                continue;
            }
            if(ch >= 'a' && ch <='z'){
                continue;
            }
            if(ch >= '0' && ch <= '9'){
                continue;
            }
            if(ch == '_' || ch == '.'){
                continue;
            }
            return false;
        }
        //TODO
        return true;
    }
    public boolean route(ExchangeType exchangeType,Binding binding,Message message) throws Exception {
        //TODO
        //根据不同的ExchangeType使用不同的转发规则
        if(exchangeType == ExchangeType.FANOUT){
            //fanout-转发到所有队列 肯定要转发
            return true;
        }else if(exchangeType == ExchangeType.TOPIC){
            return routingTopic(binding,message);
            //topic-交换更复杂
        }else {
            //不该存在的情况
            throw new Exception("[Router]交换机类型非法!ExchangeType="+exchangeType);
        }
    }
    //测试用例        routingKey         bindingKey         result
    //               aaa.bbb.*          aaa.bbb            false
    //               aaa.#              aaa                true
    private boolean routingTopic(Binding binding, Message message) {
        //TODO
        String[] bindingToken = binding.getBindingKey().split("\\.");
        String[] routingToken = message.getRoutingKey().split("\\.");
        int bindingIndex = 0,routingIndex = 0;
        while (bindingIndex < bindingToken.length && routingIndex < routingToken.length){
            //bindingKey为*,双方下标都往前移动一个单位
            if(bindingToken[bindingIndex].equals("*")){
                bindingIndex++;
                routingIndex++;

            }else if(bindingToken[bindingIndex].equals("#")){
                //bindingKey为#,若bindingKey后面没有内容则直接返回true
                bindingIndex++;
                if(bindingIndex == bindingToken.length) {
                    return true;
                }
                //bindingKey后面有内容,从routingKey中找对应的匹配
                routingIndex = findNextMatch(routingToken,routingIndex,bindingToken[bindingIndex]);
                if(routingIndex == -1){
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }else {
                //普通字符串,要求bindingKey内容和routingKey内容完全一致
                if(!bindingToken[bindingIndex].equals(routingToken[routingIndex])){
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }
        }
        if(bindingIndex == bindingToken.length && routingIndex == routingToken.length){
            return true;
        }
        return false;
    }

    private int findNextMatch(String[] routingToken, int routingIndex, String bindingChar) {
        for (int i = routingIndex; i < routingToken.length; i++) {
            if(routingToken[i].equals(bindingChar)){
                return i;
            }
        }
        return -1;
    }
}
