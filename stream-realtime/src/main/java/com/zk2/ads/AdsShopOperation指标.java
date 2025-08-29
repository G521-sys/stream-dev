package com.zk2.ads;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.sql.PreparedStatement;
import java.time.Duration;

/**
 * 店铺运营ADS层：计算核心业务指标
 */
public class AdsShopOperation指标 {
    // 输入Kafka主题（DWS层聚合数据）
    private static final String INPUT_TOPIC = ConfigUtils.getString("kafka.dws.shop.behavior.agg.topic");
    // 数据库配置
    private static final String JDBC_URL = ConfigUtils.getString("jdbc.url");
    private static final String JDBC_USER = ConfigUtils.getString("jdbc.user");
    private static final String JDBC_PASSWORD = ConfigUtils.getString("jdbc.password");

    public static void main(String[] args) throws Exception {
        // 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取DWS层数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        INPUT_TOPIC,
                        "ads_shop_operation_consumer",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> {
                            JSONObject json = JSONObject.parseObject(element);
                            return json.containsKey("window_end") ? json.getLong("window_end") : System.currentTimeMillis();
                        }),
                "shop_operation_ads_source"
        );

        // 3. 解析数据并分类
        SingleOutputStreamOperator<Operation指标> 指标Stream = kafkaSource
                .map(jsonStr -> {
                    JSONObject json = JSONObject.parseObject(jsonStr);
                    Operation指标 zh = new Operation指标();
                    zh.setStatTime(json.getLong("window_end"));
                    
                    // 区分用户指标和页面指标
                    if (json.containsKey("uid")) {
                        zh.setUserId(json.getString("uid"));
                        zh.setCartAddCount(json.getLongValue("cart_add_count"));
                        zh.setDetailViewCount(json.getLongValue("detail_view_count"));
                    } else if (json.containsKey("page_id")) {
                        zh.setPageId(json.getString("page_id"));
                        zh.setPv(json.getLongValue("pv"));
                        zh.setUv(json.getLongValue("uv"));
                        zh.setAvgDuringTime(json.getLongValue("avg_during_time"));
                    }
                    return zh;
                })
                .name("parse_to_operation_index");

        // 4. 按时间窗口聚合总指标
        KeyedStream<Operation指标, Long> timeKeyedStream = 指标Stream
                .keyBy(Operation指标::getStatTime);

        SingleOutputStreamOperator<Operation指标> total指标Stream = timeKeyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce(new Total指标Reduce());
        total指标Stream.print();
//
//        // 5. 输出到数据库
//        total指标Stream.addSink(JdbcSink.sink(
//                "INSERT INTO shop_operation指标 (stat_time, total_pv, total_uv, total_cart_add, total_detail_view, avg_stay_time) " +
//                        "VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
//                        "total_pv = VALUES(total_pv), total_uv = VALUES(total_uv), " +
//                        "total_cart_add = VALUES(total_cart_add), total_detail_view = VALUES(total_detail_view), " +
//                        "avg_stay_time = VALUES(avg_stay_time)",
//                (PreparedStatement stmt, Operation指标 zh) -> {
//                    stmt.setLong(1, zh.getStatTime());
//                    stmt.setLong(2, zh.getTotalPv());
//                    stmt.setLong(3, zh.getTotalUv());
//                    stmt.setLong(4, zh.getTotalCartAdd());
//                    stmt.setLong(5, zh.getTotalDetailView());
//                    stmt.setLong(6, zh.getAvgDuringTime());
//                },
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl(JDBC_URL)
//                        .withUsername(JDBC_USER)
//                        .withPassword(JDBC_PASSWORD)
//                        .build()
//        )).name("ads_指标_to_mysql");

        env.execute("ADS_Shop_Operation_指标");
    }

    // 运营指标实体类
    public static class Operation指标 {
        private long statTime;         // 统计时间
        private String userId;         // 用户ID
        private String pageId;         // 页面ID
        private long pv;               // 页面浏览量
        private long uv;               // 独立访客数
        private long cartAddCount;     // 加购数
        private long detailViewCount;  // 详情页浏览数
        private long avgDuringTime;    // 平均停留时间
        private long totalPv;          // 总PV
        private long totalUv;          // 总UV
        private long totalCartAdd;     // 总加购数
        private long totalDetailView;  // 总详情浏览数

        // getter和setter方法
        public long getStatTime() { return statTime; }
        public void setStatTime(long statTime) { this.statTime = statTime; }
        
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        
        public String getPageId() { return pageId; }
        public void setPageId(String pageId) { this.pageId = pageId; }
        
        public long getPv() { return pv; }
        public void setPv(long pv) { this.pv = pv; }
        
        public long getUv() { return uv; }
        public void setUv(long uv) { this.uv = uv; }
        
        public long getCartAddCount() { return cartAddCount; }
        public void setCartAddCount(long cartAddCount) { this.cartAddCount = cartAddCount; }
        
        public long getDetailViewCount() { return detailViewCount; }
        public void setDetailViewCount(long detailViewCount) { this.detailViewCount = detailViewCount; }
        
        public long getAvgDuringTime() { return avgDuringTime; }
        public void setAvgDuringTime(long avgDuringTime) { this.avgDuringTime = avgDuringTime; }
        
        public long getTotalPv() { return totalPv; }
        public void setTotalPv(long totalPv) { this.totalPv = totalPv; }
        
        public long getTotalUv() { return totalUv; }
        public void setTotalUv(long totalUv) { this.totalUv = totalUv; }
        
        public long getTotalCartAdd() { return totalCartAdd; }
        public void setTotalCartAdd(long totalCartAdd) { this.totalCartAdd = totalCartAdd; }
        
        public long getTotalDetailView() { return totalDetailView; }
        public void setTotalDetailView(long totalDetailView) { this.totalDetailView = totalDetailView; }
    }

    // 总指标聚合函数
    public static class Total指标Reduce implements ReduceFunction<Operation指标> {
        @Override
        public Operation指标 reduce(Operation指标 value1, Operation指标 value2) throws Exception {
            Operation指标 result = new Operation指标();
            result.setStatTime(value1.getStatTime());
            
            // 累加各项指标
            result.setTotalPv(value1.getTotalPv() + value2.getTotalPv() + value1.getPv() + value2.getPv());
            result.setTotalUv(value1.getTotalUv() + value2.getTotalUv() + value1.getUv() + value2.getUv());
            result.setTotalCartAdd(value1.getTotalCartAdd() + value2.getTotalCartAdd() + value1.getCartAddCount() + value2.getCartAddCount());
            result.setTotalDetailView(value1.getTotalDetailView() + value2.getTotalDetailView() + value1.getDetailViewCount() + value2.getDetailViewCount());
            
            // 计算平均停留时间
            long totalTime = value1.getAvgDuringTime() + value2.getAvgDuringTime();
            result.setAvgDuringTime(totalTime > 0 ? totalTime / 2 : 0);
            
            return result;
        }
    }
}