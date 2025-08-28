package com.zk2.ads;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class UserBehaviorAds {
    public static void main(String[] args) throws Exception {
        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取DWS层数据
        String kafkaBootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        String dwsTopic = ConfigUtils.getString("kafka.dws.user.behavior.topic");
        
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaBootstrapServers, dwsTopic, "ads_user_behavior_consumer",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "dws_topic_source"
        );

        // 3. 解析JSON并按日期分组聚合
        SingleOutputStreamOperator<String> statsStream = dwsStream
                .map(jsonStr -> JSONObject.parseObject(jsonStr))
                .name("parse_dws_json")
                .uid("parse_dws_json")
                .keyBy(json -> json.getString("dt")) // 按日期分组
                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<JSONObject, Map<String, Object>, String>() {
                    @Override
                    public Map<String, Object> createAccumulator() {
                        Map<String, Object> stats = new HashMap<>();
                        stats.put("dt", "");
                        stats.put("total_users", 0L);
                        stats.put("total_page_views", 0L);
                        stats.put("total_actions", 0L);
                        stats.put("total_displays", 0L);
                        stats.put("total_starts", 0L);
                        stats.put("new_users", 0L);
                        return stats;
                    }

                    @Override
                    public Map<String, Object> add(JSONObject value, Map<String, Object> accumulator) {
                        accumulator.put("dt", value.getString("dt"));
                        accumulator.put("total_users", (Long) accumulator.get("total_users") + 1);
                        accumulator.put("total_page_views", (Long) accumulator.get("total_page_views") + 
                                value.getInteger("page_view_count"));
                        accumulator.put("total_actions", (Long) accumulator.get("total_actions") + 
                                value.getInteger("action_count"));
                        accumulator.put("total_displays", (Long) accumulator.get("total_displays") + 
                                value.getInteger("display_count"));
                        accumulator.put("total_starts", (Long) accumulator.get("total_starts") + 
                                value.getInteger("start_count"));
                        
                        if (value.getBoolean("first_visit")) {
                            accumulator.put("new_users", (Long) accumulator.get("new_users") + 1);
                        }
                        return accumulator;
                    }

                    @Override
                    public String getResult(Map<String, Object> accumulator) {
                        return new JSONObject(accumulator).toString();
                    }

                    @Override
                    public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
                        a.put("total_users", (Long) a.get("total_users") + (Long) b.get("total_users"));
                        a.put("total_page_views", (Long) a.get("total_page_views") + (Long) b.get("total_page_views"));
                        a.put("total_actions", (Long) a.get("total_actions") + (Long) b.get("total_actions"));
                        a.put("total_displays", (Long) a.get("total_displays") + (Long) b.get("total_displays"));
                        a.put("total_starts", (Long) a.get("total_starts") + (Long) b.get("total_starts"));
                        a.put("new_users", (Long) a.get("new_users") + (Long) b.get("new_users"));
                        return a;
                    }
                })
                .name("ads_aggregate")
                .uid("ads_aggregate");
        statsStream.print();
        // 4. 输出到Kafka ADS主题或写入数据库
//        statsStream
//                .sinkTo(KafkaUtils.buildKafkaSink(kafkaBootstrapServers, ConfigUtils.getString("kafka.ads.user.behavior.topic")))
//                .name("sink_to_ads")
//                .uid("sink_to_ads");

        env.execute("UserBehaviorAdsJob");
    }
}