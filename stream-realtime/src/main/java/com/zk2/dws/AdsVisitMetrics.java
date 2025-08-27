package com.zk2.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AdsVisitMetrics {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 注册DWD层访问行为表（假设从Kafka读取）
        tEnv.executeSql("CREATE TABLE dwd_visit_behavior (" +
                "  uid STRING," +
                "  mid STRING," +
                "  sid STRING," +
                "  page_id STRING," +
                "  item STRING," +
                "  item_type STRING," +
                "  ts BIGINT," +
                "  visit_date STRING," +
                "  event_time AS TO_TIMESTAMP_LTZ(ts, 3)," +
                "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + com.stream.common.utils.ConfigUtils.getString("kafka.dwd.visit.topic") + "'," +
                "  'properties.bootstrap.servers' = '" + com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers") + "'," +
                "  'properties.group.id' = 'ads_visit_metrics'," +
                "  'format' = 'json'," +
                "  'scan.startup.mode' = 'earliest-offset'" +
                ")"
        );
        tEnv.executeSql("select * from dwd_visit_behavior").print();
        // 3. 注册订单支付表（假设存在，用于关联支付数据）
        tEnv.executeSql("CREATE TABLE dwd_payment_info (" +
                "  uid STRING," +
                "  mid STRING," +
                "  order_id STRING," +
                "  pay_amount DECIMAL(10,2)," +
                "  pay_time BIGINT," +
                "  pay_date STRING," +
                "  event_time AS TO_TIMESTAMP_LTZ(pay_time, 3)," +
                "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + com.stream.common.utils.ConfigUtils.getString("kafka.dwd.payment.topic") + "'," +
                "  'properties.bootstrap.servers' = '" + com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers") + "'," +
                "  'properties.group.id' = 'ads_payment_metrics'," +
                "  'format' = 'json'," +
                "  'scan.startup.mode' = 'earliest-offset'" +
                ")"
        );

        // 4. 注册用户会员表（存储用户交易总额和交易次数）
        tEnv.executeSql("CREATE TABLE dim_user_member (" +
                "  uid STRING PRIMARY KEY," +
                "  total_amount DECIMAL(10,2)," +
                "  total_count INT," +
                "  is_fans BOOLEAN," +
                "  update_time TIMESTAMP" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = '" + com.stream.common.utils.ConfigUtils.getString("jdbc.url") + "'," +
                "  'table-name' = 'dim_user_member'," +
                "  'username' = '" + com.stream.common.utils.ConfigUtils.getString("jdbc.username") + "'," +
                "  'password' = '" + com.stream.common.utils.ConfigUtils.getString("jdbc.password") + "'" +
                ")"
        );

        // 5. 计算核心指标并整合到一个结果表中
        Table adsResult = tEnv.sqlQuery("" +
                "WITH history_visits AS (" +
                "  SELECT DISTINCT uid, mid " +
                "  FROM dwd_visit_behavior " +
                "  WHERE visit_date < CURRENT_DATE " + // 历史访问
                "), history_payments AS (" +
                "  SELECT DISTINCT uid, mid " +
                "  FROM dwd_payment_info " +
                "  WHERE pay_date < CURRENT_DATE " + // 历史购买
                "), non_pay_users AS (" +
                "  SELECT h.uid, h.mid " +
                "  FROM history_visits h " +
                "  LEFT JOIN history_payments p ON h.uid = p.uid AND h.mid = p.mid " +
                "  WHERE p.uid IS NULL " + // 历史未购买
                "), current_visits AS (" +
                "  SELECT DISTINCT uid, mid, visit_date " +
                "  FROM dwd_visit_behavior " +
                "  WHERE visit_date = CURRENT_DATE " + // 当日访问
                "), revisit_users AS (" +
                "  SELECT DISTINCT c.uid, c.mid, c.visit_date " +
                "  FROM current_visits c " +
                "  JOIN non_pay_users n ON c.uid = n.uid AND c.mid = n.mid" +
                "), revisit_non_pay_count AS (" +
                "  SELECT " +
                "    visit_date, " +
                "    COUNT(DISTINCT uid) AS revisit_non_pay_count " +
                "  FROM revisit_users " +
                "  GROUP BY visit_date" +
                "), revisit_pay_users AS (" +
                "  SELECT DISTINCT " +
                "    r.visit_date, " +
                "    r.uid, " +
                "    r.mid " +
                "  FROM revisit_users r " +
                "  JOIN dwd_payment_info p " +
                "    ON r.uid = p.uid " +
                "    AND r.mid = p.mid " +
                "    AND r.visit_date = p.pay_date" +
                "), revisit_pay_count AS (" +
                "  SELECT " +
                "    visit_date, " +
                "    COUNT(DISTINCT uid) AS revisit_pay_buyer_count, " +
                "    SUM(pay_amount) AS revisit_pay_amount " +
                "  FROM revisit_pay_users r " +
                "  JOIN dwd_payment_info p " +
                "    ON r.uid = p.uid " +
                "    AND r.mid = p.mid " +
                "    AND r.visit_date = p.pay_date " +
                "  GROUP BY visit_date" +
                "), total_sales AS (" +
                "  SELECT " +
                "    pay_date AS visit_date, " +
                "    SUM(pay_amount) AS total_sales " +
                "  FROM dwd_payment_info " +
                "  WHERE pay_date = CURRENT_DATE " +
                "  GROUP BY pay_date" +
                "), revisit_user_tags AS (" +
                "  SELECT " +
                "    r.visit_date, " +
                "    COUNT(DISTINCT CASE WHEN m.is_fans THEN r.uid END) AS revisit_fans_count, " +
                "    COUNT(DISTINCT CASE WHEN m.total_amount > 5000 OR m.total_count > 3 THEN r.uid END) AS revisit_member_count " +
                "  FROM revisit_users r " +
                "  LEFT JOIN dim_user_member m ON r.uid = m.uid " +
                "  GROUP BY r.visit_date" +
                ")" +
                "SELECT " +
                "  COALESCE(r.visit_date, p.visit_date, t.visit_date, u.visit_date) AS stat_date, " +
                "  COALESCE(r.revisit_non_pay_count, 0) AS revisit_non_pay_count, " +
                "  CASE WHEN COALESCE(r.revisit_non_pay_count, 0) > 0 " +
                "    THEN ROUND(COALESCE(p.revisit_pay_buyer_count, 0) / r.revisit_non_pay_count, 4) " +
                "    ELSE 0 END AS revisit_pay_conversion_rate, " +
                "  CASE WHEN COALESCE(t.total_sales, 0) > 0 " +
                "    THEN ROUND(COALESCE(p.revisit_pay_amount, 0) / t.total_sales, 4) " +
                "    ELSE 0 END AS revisit_pay_amount_ratio, " +
                "  CASE WHEN COALESCE(p.revisit_pay_buyer_count, 0) > 0 " +
                "    THEN ROUND(COALESCE(p.revisit_pay_amount, 0) / p.revisit_pay_buyer_count, 2) " +
                "    ELSE 0 END AS revisit_avg_price, " +
                "  COALESCE(u.revisit_fans_count, 0) AS revisit_fans_count, " +
                "  COALESCE(u.revisit_member_count, 0) AS revisit_member_count " +
                "FROM revisit_non_pay_count r " +
                "FULL JOIN revisit_pay_count p ON r.visit_date = p.visit_date " +
                "FULL JOIN total_sales t ON COALESCE(r.visit_date, p.visit_date) = t.visit_date " +
                "FULL JOIN revisit_user_tags u ON COALESCE(r.visit_date, p.visit_date, t.visit_date) = u.visit_date"
        );

        // 6. 输出结果（可替换为实际存储介质）
        adsResult.execute().print();
        
        // 8. 执行任务
        env.execute("ADS Visit Metrics Calculation");
    }
}