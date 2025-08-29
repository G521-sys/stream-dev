package com.zk2.dws;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 店铺用户行为DWS层：聚合用户行为指标
 */
public class DwsShopCustomerBehaviorAgg {
    // 输入Kafka主题（DWD层清洗后的数据）
    private static final String INPUT_TOPIC = ConfigUtils.getString("kafka.dwd.shop.behavior.topic");
    // 输出Kafka主题（DWS层聚合数据）
    private static final String OUTPUT_TOPIC = ConfigUtils.getString("kafka.dws.shop.behavior.agg.topic");

    public static void main(String[] args) throws Exception {
        // 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取DWD层数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        INPUT_TOPIC,
                        "dws_shop_customer_consumer",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) ->
                                JSONObject.parseObject(element).getLong("ts")
                        ),
                "shop_behavior_dws_source"
        );

        // 3. 转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaSource
                .map(JSONObject::parseObject)
                .name("parse_json_to_object");

        // 4. 按用户ID分组，计算用户行为指标
        KeyedStream<JSONObject, String> userKeyedStream = jsonStream
                .filter(s -> s.getString("uid") != null)
                .keyBy((KeySelector<JSONObject, String>) json -> json.getString("uid"));

        // 5. 10分钟滚动窗口聚合（实际业务中建议改回分钟级窗口，这里保留2秒方便测试）
        SingleOutputStreamOperator<UserBehaviorAgg> userAggStream = userKeyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new UserBehaviorAggregate());

        // 6. 将聚合结果转换为JSON字符串
        SingleOutputStreamOperator<String> userJsonStream = userAggStream
                .map(UserBehaviorAgg::toJsonString)
                .name("user_agg_to_json");
//        userJsonStream.print();
        // 7. 输出到Kafka（确保最终输出为JSON字符串）
        userJsonStream.sinkTo(KafkaUtils.buildKafkaSink(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        OUTPUT_TOPIC
                ))
                .name("dws_shop_behavior_sink");

        // 打印JSON格式结果（方便调试）
//        userJsonStream.print("DWS输出JSON: ");

        env.execute("DWS_Shop_Customer_Behavior_Agg");
    }

    // 用户行为聚合实体
    public static class UserBehaviorAgg {
        private String uid;
        private long totalVisit;
        private long cartAddCount;
        private long detailViewCount;
        private long windowEnd;

        // getter/setter
        public String getUid() { return uid; }
        public void setUid(String uid) { this.uid = uid; }
        public long getTotalVisit() { return totalVisit; }
        public void setTotalVisit(long totalVisit) { this.totalVisit = totalVisit; }
        public long getCartAddCount() { return cartAddCount; }
        public void setCartAddCount(long cartAddCount) { this.cartAddCount = cartAddCount; }
        public long getDetailViewCount() { return detailViewCount; }
        public void setDetailViewCount(long detailViewCount) { this.detailViewCount = detailViewCount; }
        public long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

        // 转换为JSON字符串的核心方法
        public String toJsonString() {
            JSONObject json = new JSONObject();
            json.put("uid", uid);
            json.put("total_visit", totalVisit);
            json.put("cart_add_count", cartAddCount);
            json.put("detail_view_count", detailViewCount);
            json.put("window_end", windowEnd);
            return json.toString();
        }
    }

    // 用户行为聚合函数（补充窗口结束时间设置）
    public static class UserBehaviorAggregate implements AggregateFunction<JSONObject, UserBehaviorAgg, UserBehaviorAgg> {
        @Override
        public UserBehaviorAgg createAccumulator() {
            UserBehaviorAgg agg = new UserBehaviorAgg();
            agg.setTotalVisit(0);
            agg.setCartAddCount(0);
            agg.setDetailViewCount(0);
            return agg;
        }

        @Override
        public UserBehaviorAgg add(JSONObject value, UserBehaviorAgg accumulator) {
            accumulator.setUid(value.getString("uid"));
            accumulator.setTotalVisit(accumulator.getTotalVisit() + 1);

            String behaviorType = value.getString("behavior_type");
            if ("cart_add".equals(behaviorType)) {
                accumulator.setCartAddCount(accumulator.getCartAddCount() + 1);
            } else if ("good_detail".equals(behaviorType)) {
                accumulator.setDetailViewCount(accumulator.getDetailViewCount() + 1);
            }
            return accumulator;
        }

        @Override
        public UserBehaviorAgg getResult(UserBehaviorAgg accumulator) {
            // 可以在这里补充设置窗口结束时间（如果需要）
            // accumulator.setWindowEnd(System.currentTimeMillis());
            return accumulator;
        }

        @Override
        public UserBehaviorAgg merge(UserBehaviorAgg a, UserBehaviorAgg b) {
            a.setTotalVisit(a.getTotalVisit() + b.getTotalVisit());
            a.setCartAddCount(a.getCartAddCount() + b.getCartAddCount());
            a.setDetailViewCount(a.getDetailViewCount() + b.getDetailViewCount());
            return a;
        }
    }
}