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
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Package com.retailersv1.DbusCdc2DimHbaseAnd2DbKafka
 * @Author zhou.han
 * @Date 2024/12/12 12:56
 * @description: mysql db cdc to kafka realtime_db topic Task-01
 */
public class DbusCdc2DimHbaseAnd2DbKafka {
    // Zookeeper 地址（用于 HBase 集群连接）
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    // Kafka 集群地址（用于业务数据输出）
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    // HBase 命名空间
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    // MySQL CDC 数据输出到 Kafka 的目标主题
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 设置 Hadoop 用户名（解决 HBase/Kafka 访问权限问题，避免权限校验失败）
        System.setProperty("HADOOP_USER_NAME","root");
        // 2. 初始化 Flink 流处理环境（默认使用本地/集群环境，注释的代码用于加载默认参数如并行度、检查点等）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);// 可选：配置检查点、并行度等默认参数

        // 业务库 CDC 数据源
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        // 读取配置库的变化binlog
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        // 转换为 Flink 数据流（无水印，CDC 数据自带时序信息）
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource,WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");
//        cdcDbMainStream.print();
//        cdcDbDimStream.print();
        //读取到的Mysql的数据转换成json的形式
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);
//        cdcDbMainStreamMap.print();
        //将json的数据存入到kafka
        cdcDbMainStreamMap.map(JSONObject::toString)
        .sinkTo(
                        KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

//        cdcDbMainStreamMap.print("cdcDbMainStreamMap -> ");


        //将mysql的数据转换成json
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);
//        cdcDbMainStreamMap.print();
        //对json格式进行处理
        //{"op":"r","after":{"birthday":7107,"create_time":1755557118000,"login_name":"d0aoph","nick_name":"阿生","name":"任生","user_level":"2","phone_num":"13149134291","id":211,"email":"re6xwiqpqn@aol.com","operate_time":1755475200000},"source":{"server_id":0,"version":"1.6.4.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"realtime_v1","table":"user_info"},"ts_ms":1755584737663}
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
//                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");
        //{"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_activity_rule","source_table":"activity_rule","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level"}}
        cdcDbDimStreamMapCleanColumn.print("cdcDbDimStreamMapCleanColumn==>");
        // 根据配置创建/更新 HBase 维度表
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");


        // 创建Map状态描述符
        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        // 将配置数据流转换为广播流
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
        // 连接业务数据流和广播流
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);
        // 处理连接后的流
        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));

        // 禁用算子链，方便调试
        env.disableOperatorChaining();
        env.execute();
    }

}
