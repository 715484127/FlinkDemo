package org.example.demo01.deserializer;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.KeyDeserializers;
import org.apache.flink.util.Collector;
import org.example.demo01.beans.Order;

import java.io.IOException;

public class MyDeserializer<T> implements DeserializationSchema<Order> {

    @Override
    public Order deserialize(byte[] bytes) throws IOException {
        String msg = new String(bytes);
        Order data = JSON.parseObject(msg, Order.class);
        return data;
    }


    @Override
    public boolean isEndOfStream(Order s) {
        return false;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return null;
    }
}
