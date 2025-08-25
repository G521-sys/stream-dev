package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdPaymentInfoSQL {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_ORDER_TOPIC = ConfigUtils.getString("kafka.dwd.order.detail");
    private static final String DWD_PYAMENTINFO_TOPIC = ConfigUtils.getString("kafka.dwd.payment.info");


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStateBackend(new MemoryStateBackend());

        tableEnv.executeSql("CREATE TABLE ods_professional (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")" + SQLUtil.getKafka(ODS_KAFKA_TOPIC, "test"));
        // 1. 读取下单事务事实表
        tableEnv.executeSql(
                "create table dwd_order_info(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                        " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                        ")" + SQLUtil.getKafka(DWD_ORDER_TOPIC, "test"));

//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // 2. 从 ods_ecommerce_order 中过滤 payment_info
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "`after`['user_id'] user_id," +
                "`after`['order_id'] order_id," +
                "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                "`after`['callback_time'] callback_time," +
                "`proc_time`," +
                "ts_ms, " +
                "time_ltz " +
                "from ods_professional " +
                "where `source`['table'] = 'payment_info' " +
                "and `op` = 'u' " +
                "and `after`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

//        paymentInfo.execute().print();

        //3 从 hbase 中读取 字典数据 创建 字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SQLUtil.getHbaseDDL("dim_base_dic"));

//        tableEnv.executeSql("select * from base_dic").print();

        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts_ms " +
                        "from payment_info pi " +
                        "join dwd_order_info od " +
                        "on pi.order_id=od.order_id " +
                        "and od.time_ltz >= pi.time_ltz - interval '30' minute " +
                        "and od.time_ltz <= pi.time_ltz + interval '5' second " +
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");

//        result.execute().print();

        tableEnv.executeSql("create table "+DWD_PYAMENTINFO_TOPIC+"(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint ," +
                "primary key(order_detail_id) not enforced " +
                ")" + SQLUtil.getUpsertKafkaDDL(DWD_PYAMENTINFO_TOPIC));

        result.executeInsert(DWD_PYAMENTINFO_TOPIC);

    }
}