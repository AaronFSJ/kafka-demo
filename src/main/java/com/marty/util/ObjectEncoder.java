package com.marty.util;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @Author: Aaron
 * @Descprition:
 * @Date: Create in 2017/12/25 15:14
 * @Modyfied By:
 */

public class ObjectEncoder<T> implements Serializer<T>{

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}