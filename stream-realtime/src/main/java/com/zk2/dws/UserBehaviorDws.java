package com.zk2.dws;

import avro.shaded.com.google.common.util.concurrent.Uninterruptibles;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class UserBehaviorDws {
    public static void main(String[] args) throws Exception {
        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取Kafka数据（来自DWD层page主题）
        String kafkaBootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        String pageTopic = ConfigUtils.getString("kafka.page.topic");
        
        DataStreamSource<String> pageStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaBootstrapServers, pageTopic, "dws_user_behavior_consumer", 
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "page_topic_source"
        );

        // 3. 转换为JSON对象并提取关键信息
        SingleOutputStreamOperator<JSONObject> jsonStream = pageStream
                .map(jsonStr -> JSONObject.parseObject(jsonStr))
                .name("parse_json")
                .uid("parse_json");

        // 4. 提取用户行为核心字段
        SingleOutputStreamOperator<Tuple3<String, String, JSONObject>> userBehaviorStream = jsonStream
                .map(new RichMapFunction<JSONObject, Tuple3<String, String, JSONObject>>() {
                    @Override
                    public Tuple3<String, String, JSONObject> map(JSONObject json) throws Exception {
                        String mid = json.getJSONObject("common").getString("mid");
                        // 修正 sleep 调用
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                        // 重新处理 dt 逻辑，这里示例用当前时间戳
                        String dt = String.valueOf(System.currentTimeMillis());
                        return new Tuple3<>(mid, dt, json);
                    }
                })
                .name("extract_user_behavior")
                .uid("extract_user_behavior");

        // 5. 按用户ID分组，10分钟滚动窗口聚合
        SingleOutputStreamOperator<JSONObject> userBehaviorWideTable = userBehaviorStream
                .keyBy(tuple -> tuple.f0) // 按mid分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
                .aggregate(new AggregateFunction<
                        Tuple3<String, String, JSONObject>,
                        JSONObject,
                        JSONObject>() {
                    @Override
                    public JSONObject createAccumulator() {
                        JSONObject accumulator = new JSONObject();
                        accumulator.put("mid", "");
                        accumulator.put("dt", "");
                        accumulator.put("page_view_count", 0);
                        accumulator.put("action_count", 0);
                        accumulator.put("display_count", 0);
                        accumulator.put("start_count", 0);
                        accumulator.put("first_visit", false);
                        return accumulator;
                    }

                    @Override
                    public JSONObject add(Tuple3<String, String, JSONObject> value, JSONObject accumulator) {
                        JSONObject data = value.f2;
                        accumulator.put("mid", value.f0);
                        accumulator.put("dt", value.f1);
                        
                        // 页面浏览数
                        accumulator.put("page_view_count", accumulator.getInteger("page_view_count") + 1);
                        
                        // 行为数
                        if (data.containsKey("actions")) {
                            accumulator.put("action_count", accumulator.getInteger("action_count") + 
                                    data.getJSONArray("actions").size());
                        }
                        
                        // 曝光数
                        if (data.containsKey("displays")) {
                            accumulator.put("display_count", accumulator.getInteger("display_count") + 
                                    data.getJSONArray("displays").size());
                        }
                        
                        // 启动数
                        if ("start".equals(data.getJSONObject("page").getString("page_id"))) {
                            accumulator.put("start_count", accumulator.getInteger("start_count") + 1);
                        }
                        
                        // 首次访问标记
                        if ("1".equals(data.getJSONObject("common").getString("is_new"))) {
                            accumulator.put("first_visit", true);
                        }
                        
                        return accumulator;
                    }

                    @Override
                    public JSONObject getResult(JSONObject accumulator) {
                        return accumulator;
                    }

                    @Override
                    public JSONObject merge(JSONObject a, JSONObject b) {
                        a.put("page_view_count", a.getInteger("page_view_count") + b.getInteger("page_view_count"));
                        a.put("action_count", a.getInteger("action_count") + b.getInteger("action_count"));
                        a.put("display_count", a.getInteger("display_count") + b.getInteger("display_count"));
                        a.put("start_count", a.getInteger("start_count") + b.getInteger("start_count"));
                        a.put("first_visit", a.getBoolean("first_visit") || b.getBoolean("first_visit"));
                        return a;
                    }
                })
                .name("user_behavior_aggregate")
                .uid("user_behavior_aggregate");

        // 6. 输出到Kafka DWS主题
        userBehaviorWideTable
                .map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaBootstrapServers, ConfigUtils.getString("kafka.dws.user.behavior.topic")))
                .name("sink_to_dws")
                .uid("sink_to_dws");

        env.execute("UserBehaviorDwsJob");
    }
}