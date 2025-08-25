package com.retailersv1.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.*;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.*;

public class DwdBaseDb{
    private static final String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String KAFKA_BOOTSTRAP_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        KAFKA_BOOTSTRAP_SERVER,
                        TOPIC_DB,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "kafka_base_db"
        );

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("非标准JSON");
                }
            }
        });

        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
//                "10000-10050"
        );
        DataStreamSource<String> cdcDwdDs = env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_source");

        SingleOutputStreamOperator<TableProcessDwd> tpDs = cdcDwdDs.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String op = jsonObject.getString("op");
                TableProcessDwd tp = null;
                if ("d".equals(op)) {
                    tp = jsonObject.getObject("before", TableProcessDwd.class);
                } else {
                    tp = jsonObject.getObject("after", TableProcessDwd.class);
                }
                tp.setOp(op);
                return tp;
            }
        });

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDs = tpDs.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDs = jsonObjDs.connect(broadcastDs);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDs = connectDs.process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
            private Map<String, TableProcessDwd> configMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection connection = JdbcUtils.getMySQLConnection(
                        ConfigUtils.getString("mysql.url"),
                        ConfigUtils.getString("mysql.user"),
                        ConfigUtils.getString("mysql.pwd"));
                String querySQL = "select * from realtime_v1_config.table_process_dwd";
                List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(connection, querySQL, TableProcessDwd.class, true);
                for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                    configMap.put(tableProcessDwd.getSourceTable(), tableProcessDwd);
                }

                connection.close();
            }

            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                String tableName = jsonObject.getJSONObject("source").getString("table");
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                TableProcessDwd tp = null;
                if ((tp = broadcastState.get(tableName)) != null || (tp = configMap.get(tableName)) != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    deleteNotNeedColumns(after, tp.getSinkColumns());
                    after.put("ts", jsonObject.getLongValue("ts_ms"));
                    collector.collect(Tuple2.of(after, tp));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                String op = tableProcessDwd.getOp();
                BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                if ("d".equals(op)) {
                    configMap.remove(tableProcessDwd.getSourceTable());
                    broadcastState.remove(tableProcessDwd.getSourceTable());
                } else {
                    configMap.put(tableProcessDwd.getSourceTable(), tableProcessDwd);
                    broadcastState.put(tableProcessDwd.getSourceTable(), tableProcessDwd);
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        splitDs.print();
        splitDs.sinkTo(KafkaUtils.getKafkaSinkDwd());

        env.execute();

    }

    private static void deleteNotNeedColumns(JSONObject jsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}

































