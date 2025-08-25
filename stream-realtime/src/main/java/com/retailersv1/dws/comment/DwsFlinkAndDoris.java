package com.retailersv1.dws.comment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsFlinkAndDoris {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L);

        tenv.executeSql("create table flink_doris(\n" +
                "    siteid int,\n" +
                "    citycode smallint,\n" +
                "    username string,\n" +
                "    pv bigint\n" +
                ") with(\n" +
                "    'connector'='doris',\n" +
                "    'fenodes'='cdh01:8031',\n" +
                "    'table.identifier'='realtime_v1.flink_doris'\n" +
                "    'username'='root',\n" +
                "    'password'=''\n" +
                "    'sink.properties.format' = 'json', " +
                "    'sink.buffer-count' = '4', " +
                "    'sink.buffer-size' = '4086', " +
                "    'sink.enable-2pc' = 'false' " +
                ")");

        tenv.executeSql("insert into flink_doris values(33,3,'深圳',3333)");

    }
}




























