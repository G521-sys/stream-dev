package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DbusCdc2DimHbaseAnd2DbKafka {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_KAFKA_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 打印配置信息用于调试
        System.out.println("MySQL配置信息：");
        System.out.println("Host: " + ConfigUtils.getString("mysql.host"));
        System.out.println("Port: " + ConfigUtils.getString("mysql.port"));
        System.out.println("Database: " + ConfigUtils.getString("mysql.database"));
        System.out.println("User: " + ConfigUtils.getString("mysql.user"));
        System.out.println("Config Database: " + ConfigUtils.getString("mysql.databases.conf"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
            // 构建CDC源
            MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                    ConfigUtils.getString("mysql.database"),
                    "", // 所有表
                    ConfigUtils.getString("mysql.user"),
                    ConfigUtils.getString("mysql.pwd"),
                    StartupOptions.initial()
            );

            MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                    ConfigUtils.getString("mysql.databases.conf"),
                    ConfigUtils.getString("mysql.databases.conf") + ".table_process_dim",
                    ConfigUtils.getString("mysql.user"),
                    ConfigUtils.getString("mysql.pwd"),
                    StartupOptions.initial()
            );

            // 创建数据源
            DataStreamSource<String> cdcDbMainStream = env.fromSource(
                    mySQLDbMainCdcSource,
                    WatermarkStrategy.noWatermarks(),
                    "mysql_cdc_main_source"
            );

            DataStreamSource<String> cdcDbDimStream = env.fromSource(
                    mySQLCdcDimConfSource,
                    WatermarkStrategy.noWatermarks(),
                    "mysql_cdc_dim_source"
            );
//        cdcDbDimStream.print("cdcDbDimStream");
//        cdcDbMainStream.print("cdcDbMainStream");


            // 数据处理流程
            SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream
                    .map(JSONObject::parseObject)
                    .uid("db_data_convert_json")
                    .name("db_data_convert_json")
                    .setParallelism(1);

            // 输出到Kafka
            cdcDbMainStreamMap.map(JSONObject::toString)
                    .sinkTo(KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC))
                    .uid("mysql_cdc_to_kafka_topic")
                    .name("mysql_cdc_to_kafka_topic");

        cdcDbMainStreamMap.print("cdcDbDimStream");
            // 维度表处理
            SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream
                    .map(JSONObject::parseObject)
                    .uid("dim_data_convert_json")
                    .name("dim_data_convert_json")
                    .setParallelism(1);
        cdcDbDimStreamMap.print("cdcDbMainStream");
            SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap
                    .map(s -> {
                        s.remove("source");
                        s.remove("transaction");
                        JSONObject resJson = new JSONObject();
                        if ("d".equals(s.getString("op"))) {
                            resJson.put("before", s.getJSONObject("before"));
                        } else {
                            resJson.put("after", s.getJSONObject("after"));
                        }
                        resJson.put("op", s.getString("op"));
                        return resJson;
                    })
                    .uid("clean_json_column_map")
                    .name("clean_json_column_map");

            SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn
                    .map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_KAFKA_NAME_SPACE))
                    .uid("map_create_hbase_dim_table")
                    .name("map_create_hbase_dim_table");

            // 广播流处理
            MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>(
                    "mapStageDesc", String.class, JSONObject.class);
            BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
            BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);

            connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));

            env.disableOperatorChaining();
            env.execute();
    }
}