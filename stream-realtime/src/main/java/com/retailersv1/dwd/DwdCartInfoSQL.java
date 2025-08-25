package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdCartInfoSQL {
    private static String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");
    private static String DWD_KAFKA_TOPIC = ConfigUtils.getString("kafka.dwd.cart.info");
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter( env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("create table ods_professional(\n" +
                "    `op` string,\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `ts_ms` bigint,\n" +
                "    proc_time as proctime()\n" +
                ")"+ SQLUtil.getKafka(TOPIC_DB, "test"));

        Table cartInfo = tenv.sqlQuery("select\n" +
                "    `after`['id'] id,\n" +
                "    `after`['user_id'] user_id,\n" +
                "    `after`['sku_id'] sku_id,\n" +
                "    if(`op`='r',`after`['sku_num'],cast((cast(after['sku_num'] as int) - cast(`before`['sku_num'] as int)) as string)) sku_num,\n" +
                "    ts_ms as ts\n" +
                "from ods_professional\n" +
                "where `source`['table']='cart_info'\n" +
                "and (\n" +
                "    `op`='r' or\n" +
                "    `op`='u' and `after`['sku_num'] is not null and (cast (`after`['sku_num'] as int) > cast (`before`['sku_num'] as int))\n" +  // 将old改为before
                "    )");

//        cartInfo.execute().print();

        // 修改输出表的创建语句，添加主键约束
        tenv.executeSql("create table "+DWD_KAFKA_TOPIC+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED \n" +
                ")"+ SQLUtil.getUpsertKafkaDDL(DWD_KAFKA_TOPIC));

        cartInfo.executeInsert(DWD_KAFKA_TOPIC);

    }
}