package org.example.demo01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.ACLable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.demo01.beans.Order;
import org.example.demo01.deserializer.MyDeserializer;

import java.util.Properties;

public class FlinkApplication01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Kafka数据源 新版API https://cwiki.apache.org/confluence/display/FLINK/FLIP-27:+Refactor+Source+Interface
        //https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/sources/
        KafkaSource source = KafkaSource.<Order>builder().
                setProperties(properties).
                setTopics("my-topic-test-1").
                setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)).
                setValueOnlyDeserializer(new MyDeserializer()).build();
        //新版API使用fromSource
        DataStreamSource<Order> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(),"Kafka Source", TypeInformation.of(Order.class));

        //自定义数据源
        DataStreamSource<Order> dataStreamSource1 = env.addSource(new SourceFunction<Order>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                int id = 1;
                while (running){
                    Order order = new Order(String.valueOf(id),id++);
                    sourceContext.collect(order);
                    Thread.sleep(100L);
                    if(id==10){
                        running = false;
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        dataStreamSource1.map(order -> Tuple3.of(order.getId(),order.getAmount(),1)).returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
                .keyBy(t->true).reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
                        return Tuple3.of("total",t1.f1+t2.f1,t1.f2+t2.f2);
                    }
                }).print();



        //单流转换算子fliter、map、flatMap

        //dataStreamSource.map(order -> order.getAmount()).returns(Integer.class).print();
        //dataStreamSource.map(order -> Tuple2.of(order.getId(),order.getAmount())).returns(Types.TUPLE(Types.STRING,Types.INT)).print();
        //dataStreamSource.filter(order -> order.getAmount() > 100).print();

        //dataStreamSource.flatMap((Order order, Collector<Integer> out) -> out.collect(order.getAmount())).returns(Integer.class).print();

//        dataStreamSource.flatMap(new FlatMapFunction<Order, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(Order order, Collector<Tuple2<String,Integer>> collector) throws Exception {
//                if(order.getAmount() > 100){
//                    collector.collect(Tuple2.of(order.getId(),order.getAmount()));
//                }else{
//                    collector.collect(Tuple2.of(order.getId(),order.getAmount()));
//                    collector.collect(Tuple2.of(order.getId(),order.getAmount()));
//                }
//            }
//        }).print();

        //滚动聚合
//        dataStreamSource.map(order -> Tuple2.of(order.getId(),order.getAmount())).returns(Types.TUPLE(Types.STRING,Types.INT))
//                .keyBy(t->t.f0).sum(1).print();

//        dataStreamSource.map(order -> Tuple2.of(order.getId(),order.getAmount())).returns(Types.TUPLE(Types.STRING,Types.INT))
//                .keyBy(t->t.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//                        return ;
//                    }
//                }).print();


        env.execute();
    }
}
