package com.zk2.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AdsShopNewVisitorMetrics {
    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 2. 注册DWD层行为数据视图（修复TO_TIMESTAMP_LTZ参数问题）
        tEnv.executeSql("CREATE TABLE dwd_shop_customer_behavior (\n" +
                "                    uid STRING,\n" +
                "                    is_new STRING,\n" +
                "                    behavior_type STRING,\n" +
                "                    ts BIGINT,\n" +
                "                    item STRING,\n" +
                "                    item_type STRING,\n" +
                "                    -- 增加时间单位参数（假设ts是毫秒级时间戳）\n" +
                "                    event_time AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "                    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                "                ) WITH (\n" +
                "                    'connector' = 'kafka',\n" +
                "                    'topic' = 'dwd_shop_behavior',\n" +
                "                    'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh3:9092',\n" +
                "                    'properties.group.id' = 'ads_new_visitor_group',\n" +
                "                    'format' = 'json',\n" +
                "                    'scan.startup.mode' = 'earliest-offset'\n" +
                "                )");

        // 3. 计算基础指标（按天聚合）
        Table baseMetrics = tEnv.sqlQuery("WITH daily_behavior AS (\n" +
                "                    -- 按用户和天聚合行为\n" +
                "                    SELECT\n" +
                "                        uid,\n" +
                "                        is_new,\n" +
                "                        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS stat_date,\n" +
                "                        -- 判断是否有支付行为\n" +
                "                        MAX(CASE WHEN behavior_type = 'payment' THEN 1 ELSE 0 END) AS has_payment,\n" +
                "                        -- 判断是否有关注行为（假设关注行为标识为favor_add）\n" +
                "                        MAX(CASE WHEN behavior_type = 'favor_add' THEN 1 ELSE 0 END) AS has_favor,\n" +
                "                        -- 统计支付次数\n" +
                "                        COUNT(CASE WHEN behavior_type = 'payment' THEN 1 END) AS payment_count\n" +
                "                    FROM dwd_shop_customer_behavior\n" +
                "                    GROUP BY uid, is_new, DATE_FORMAT(event_time, 'yyyy-MM-dd')\n" +
                "                ),\n" +
                "                new_visitors AS (\n" +
                "                    -- 筛选新访用户\n" +
                "                    SELECT \n" +
                "                        uid,\n" +
                "                        stat_date,\n" +
                "                        has_payment,\n" +
                "                        has_favor,\n" +
                "                        payment_count\n" +
                "                    FROM daily_behavior\n" +
                "                    WHERE is_new = '1'\n" +
                "                ),\n" +
                "                total_payment AS (\n" +
                "                    -- 全量用户支付次数（用于会员计算）\n" +
                "                    SELECT\n" +
                "                        uid,\n" +
                "                        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS stat_date,\n" +
                "                        COUNT(CASE WHEN behavior_type = 'payment' THEN 1 END) AS total_payment_count\n" +
                "                    FROM dwd_shop_customer_behavior\n" +
                "                    GROUP BY uid, DATE_FORMAT(event_time, 'yyyy-MM-dd')\n" +
                "                )\n" +
                "                SELECT\n" +
                "                    stat_date,\n" +
                "                    -- 新访访客数\n" +
                "                    COUNT(DISTINCT uid) AS new_visitor_count,\n" +
                "                    -- 新访成交买家数\n" +
                "                    COUNT(DISTINCT CASE WHEN has_payment = 1 THEN uid END) AS new_pay_buyer_count,\n" +
                "                    -- 新访未成交访客数\n" +
                "                    COUNT(DISTINCT CASE WHEN has_payment = 0 THEN uid END) AS new_non_pay_visitor_count,\n" +
                "                    -- 新访支付转化率\n" +
                "                    ROUND(\n" +
                "                        COUNT(DISTINCT CASE WHEN has_payment = 1 THEN uid END) \n" +
                "                        / COUNT(DISTINCT uid) * 100, \n" +
                "                        2\n" +
                "                    ) AS new_pay_conversion_rate,\n" +
                "                    -- 新访粉丝数\n" +
                "                    COUNT(DISTINCT CASE WHEN has_favor = 1 THEN uid END) AS new_fans_count,\n" +
                "                    -- 新访会员数（支付次数>3）\n" +
                "                    COUNT(DISTINCT CASE WHEN payment_count > 3 THEN uid END) AS new_member_count\n" +
                "                FROM new_visitors\n" +
                "                GROUP BY stat_date");
        baseMetrics.execute().print();
//        // 4. 输出结果到ADS层（可根据实际需求选择存储介质）
//        tEnv.executeSql("CREATE TABLE ads_new_visitor_metrics (\n" +
//                "                    stat_date STRING,\n" +
//                "                    new_visitor_count BIGINT,\n" +
//                "                    new_pay_buyer_count BIGINT,\n" +
//                "                    new_non_pay_visitor_count BIGINT,\n" +
//                "                    new_pay_conversion_rate DECIMAL(5,2),\n" +
//                "                    new_fans_count BIGINT,\n" +
//                "                    new_member_count BIGINT,\n" +
//                "                    etl_time AS CURRENT_TIMESTAMP(3)\n" +
//                "                ) WITH (\n" +
//                "                    'connector' = 'jdbc',\n" +
//                "                    'url' = 'jdbc:hive2://cdh01:10000',\n" +
//                "                    'table-name' = 'ads_new_visitor_metrics',\n" +
//                "                    'username' = '${jdbc.username}',\n" +
//                "                    'password' = '${jdbc.password}'\n" +
//                "                )");

        // 5. 插入计算结果
//        baseMetrics.executeInsert("ads_new_visitor_metrics");

        env.execute("ADS_Shop_New_Visitor_Metrics");
    }
}