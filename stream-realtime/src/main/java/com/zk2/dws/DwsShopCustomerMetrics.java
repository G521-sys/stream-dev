package com.zk2.dws;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DwsShopCustomerMetrics {
    // 输入Kafka主题（DWD层清洗后的数据）
    private static final String INPUT_TOPIC = ConfigUtils.getString("kafka.dwd.shop.behavior.topic");
    // 输出Kafka主题（DWS层聚合结果）
    private static final String OUTPUT_TOPIC = ConfigUtils.getString("kafka.dws.shop.metrics.topic");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取DWD层数据
        DataStreamSource<String> dwdSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        INPUT_TOPIC,
                        "dws_shop_metrics_consumer",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> JSONObject.parseObject(event).getLong("ts")),
                "dws_shop_source"
        );

        // 转换为JSON对象并按店铺+日期分组
        KeyedStream<Tuple3<String, String, JSONObject>, String> keyedStream = dwdSource
                .map(new MapFunction<String, Tuple3<String, String, JSONObject>>() {
                    @Override
                    public Tuple3<String, String, JSONObject> map(String s) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        // 从数据中提取店铺ID、日期和完整JSON对象
                        String uid = jsonObject.getString("uid"); // 数据中使用uid作为店铺标识
                        String dt = DateTimeUtils.tsToDate(jsonObject.getLong("ts")); // 转换为yyyy-MM-dd
                        return Tuple3.of(uid, dt, jsonObject);
                    }
                })
                .keyBy(tuple -> tuple.f0 + "-" + tuple.f1); // 按shop_id+dt分组

        // 窗口聚合计算（天级窗口）
        SingleOutputStreamOperator<JSONObject> metricsStream = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .aggregate(new ShopCustomerAggregate());
        metricsStream.print();
//        // 输出到Kafka
//        metricsStream.map(JSONObject::toString)
//                .sinkTo(KafkaUtils.buildKafkaSink(
//                        ConfigUtils.getString("kafka.bootstrap.servers"),
//                        OUTPUT_TOPIC
//                )).name("dws_shop_metrics_sink");

        env.execute("DWS_Shop_Customer_Metrics_Calc");
    }

    // 聚合函数实现指标计算
    public static class ShopCustomerAggregate implements AggregateFunction<
            Tuple3<String, String, JSONObject>,  // 输入：(shop_id, dt, 行为数据)
            MetricsAccumulator,                  // 累加器
            JSONObject                           // 输出：聚合结果
            > {

        @Override
        public MetricsAccumulator createAccumulator() {
            return new MetricsAccumulator();
        }

        @Override
        public MetricsAccumulator add(Tuple3<String, String, JSONObject> value, MetricsAccumulator accumulator) {
            String uid = value.f0;
            String dt = value.f1;
            JSONObject behavior = value.f2;

            // 初始化累加器的店铺ID和日期
            accumulator.uid = uid;
            accumulator.dt = dt;

            // 从数据中提取所需字段
            String mid = behavior.getString("mid"); // 用户标识
            String behaviorType = behavior.getString("behavior_type"); // 行为类型
            boolean isNew = "1".equals(behavior.getString("is_new")); // 是否新访

            // 提取支付金额（从item字段解析，假设item为订单ID，这里简化处理）
            double amount = 0.0;
            if ("payment".equals(behaviorType)) {
                // 实际应用中应从订单信息中获取金额，这里为演示给一个默认值
                amount = 100.0; // 实际场景需要替换为真实金额获取逻辑
            }

            // 1. 店铺客户数（访问、互动、支付行为的去重用户）
            if ("payment".equals(behaviorType) || "order".equals(behaviorType) ||
                    "order_list".equals(behaviorType)) {
                accumulator.allCustomers.add(mid);
            }

            // 2. 客户新访数
            if (isNew) {
                accumulator.newVisitors.add(mid);
            }

            // 3. 新访成交相关
            if (isNew && "payment".equals(behaviorType)) {
                accumulator.newVisitPayers.add(mid);
                accumulator.newVisitPayAmount += amount;
            }

            // 4. 总支付金额
            if ("payment".equals(behaviorType)) {
                accumulator.totalPayAmount += amount;
            }

            // 5. 新访粉丝数（数据中没有关注行为，预留逻辑）
            if (isNew && "follow".equals(behaviorType)) {
                accumulator.newFollowers.add(mid);
            }

            // 6. 新访会员数（累计支付≥5000或次数≥3）
            if (isNew) {
                if ("payment".equals(behaviorType)) {
                    accumulator.newUserPayCount.put(mid, accumulator.newUserPayCount.getOrDefault(mid, 0) + 1);
                    accumulator.newUserTotalAmount.put(mid, accumulator.newUserTotalAmount.getOrDefault(mid, 0.0) + amount);

                    // 检查是否达到会员条件
                    if (accumulator.newUserTotalAmount.get(mid) >= 5000 || accumulator.newUserPayCount.get(mid) >= 3) {
                        accumulator.newMembers.add(mid);
                    }
                }
            }

            return accumulator;
        }

        @Override
        public JSONObject getResult(MetricsAccumulator accumulator) {
            JSONObject result = new JSONObject();
            result.put("shop_id", accumulator.uid);
            result.put("dt", accumulator.dt);
            result.put("shop_customer_count", accumulator.allCustomers.size()); // 店铺客户数
            result.put("new_visitor_count", accumulator.newVisitors.size()); // 客户新访数
            result.put("new_visit_pay_count", accumulator.newVisitPayers.size()); // 新访成交数

            // 计算新访支付转化率（避免除零）
            result.put("new_visit_pay_rate", accumulator.newVisitors.isEmpty() ? 0 :
                    (double) accumulator.newVisitPayers.size() / accumulator.newVisitors.size());

            // 计算新访支付金额占比（避免除零）
            result.put("new_visit_pay_ratio", accumulator.totalPayAmount == 0 ? 0 :
                    accumulator.newVisitPayAmount / accumulator.totalPayAmount);

            // 计算新访客单价（避免除零）
            result.put("new_visit_avg_price", accumulator.newVisitPayers.isEmpty() ? 0 :
                    accumulator.newVisitPayAmount / accumulator.newVisitPayers.size());

            result.put("new_visit_follower_count", accumulator.newFollowers.size()); // 新访粉丝数
            result.put("new_visit_member_count", accumulator.newMembers.size()); // 新访会员数

            return result;
        }

        @Override
        public MetricsAccumulator merge(MetricsAccumulator a, MetricsAccumulator b) {
            a.allCustomers.addAll(b.allCustomers);
            a.newVisitors.addAll(b.newVisitors);
            a.newVisitPayers.addAll(b.newVisitPayers);
            a.newFollowers.addAll(b.newFollowers);
            a.newMembers.addAll(b.newMembers);
            a.newVisitPayAmount += b.newVisitPayAmount;
            a.totalPayAmount += b.totalPayAmount;

            // 合并新用户支付次数和金额
            b.newUserPayCount.forEach((k, v) -> a.newUserPayCount.put(k, a.newUserPayCount.getOrDefault(k, 0) + v));
            b.newUserTotalAmount.forEach((k, v) -> a.newUserTotalAmount.put(k, a.newUserTotalAmount.getOrDefault(k, 0.0) + v));

            return a;
        }
    }

    // 累加器类（存储中间计算结果）
    public static class MetricsAccumulator {
        String uid;
        String dt;
        Set<String> allCustomers = new HashSet<>(); // 店铺客户（去重）
        Set<String> newVisitors = new HashSet<>(); // 新访用户
        Set<String> newVisitPayers = new HashSet<>(); // 新访成交用户
        Set<String> newFollowers = new HashSet<>(); // 新访粉丝
        Set<String> newMembers = new HashSet<>(); // 新访会员
        double newVisitPayAmount = 0.0; // 新访支付总金额
        double totalPayAmount = 0.0; // 店铺总支付金额
        Map<String, Integer> newUserPayCount = new HashMap<>(); // 新用户支付次数
        Map<String, Double> newUserTotalAmount = new HashMap<>(); // 新用户支付总额
    }
}